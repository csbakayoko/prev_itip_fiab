"""
Pipeline de fiabilisation ITIP-FIAB — point d'entrée.

Spine essentiel, multi-périmètre :
    load → clean → waterfall → synthèse (ASCII console) + analyses par clause

Périmètre piloté par config/profile.py (par défaut : toutes les clauses). Lancement :
    spark-submit main.py        (ou exécution dans un notebook Databricks)
"""

import logging

from pyspark.sql import DataFrame, SparkSession

from config import (
    db_cfg, tech_cfg, RUN_PARAMS,
    EXPORT_ANALYSES, EXPORT_FORMATS, EXPORT_DELTA_SCHEMA,
)
from modules.load_data import load_cpt_raw, load_mrm_raw
from modules.transform import clean_cpt, clean_mrm
from modules.matching import matching_waterfall, recover_late_declarations
from modules.analysis import (
    flag_late_it_observations,
    enrich_result_tags,
    drop_unmatched_inventory_non,
    diagnose_mrm_fanout,
    restituer_analyses,
    export_analyses,
)
from modules.kpi_export import print_synthese
from modules._timing import timed
import pyspark.sql.functions as F


def run(spark: SparkSession) -> DataFrame:
    """Exécute le pipeline complet et affiche la synthèse client."""
    with timed("PIPELINE TOTAL"):
        cpt_clean = clean_cpt(load_cpt_raw(spark, db_cfg), tech_cfg)
        mrm_clean = clean_mrm(load_mrm_raw(spark, db_cfg), tech_cfg)

        df_result = matching_waterfall(cpt_clean, mrm_clean)

        # Déclarations tardives : CPT_ONLY retrouvés dans l'inventaire MRM N+1.
        # Les dossiers récupérés sont enrichis des infos MRM (TYPE_RECONCILIATION=CPT_LATE).
        if RUN_PARAMS.get("fichier_mrm_n1"):
            mrm_n1 = clean_mrm(load_mrm_raw(spark, db_cfg, "fichier_mrm_n1"), tech_cfg)
            df_result = recover_late_declarations(df_result, [("MRM_N1", mrm_n1)])

        # Observations tardives IT : CPT_ONLY garantie 60 survenus en fin d'année,
        # absents du MRM courant et du N+1 → tagués CPT_OBS_TARDIVE (anomalie,
        # LATE_SOURCE=OBS_TARDIVE_IT). Jamais matchés → exclus des taux et des PM.
        df_result = flag_late_it_observations(df_result)

        # Colonnes persistantes : consigne reformatée (MRM_ACTION) + tag des
        # orphelins CPT_ONLY (TAG_CPT_ONLY).
        df_result = enrich_result_tags(df_result)

        # Nettoyage final : le statut inventaire NON est ouvert au load pour le
        # repêchage (un NON qui matche est conservé). Les MRM NON restés NON
        # matchés (MRM_MISSING) n'ont aucune contrepartie compte → jetés (hors
        # métriques et hors export).
        df_result = drop_unmatched_inventory_non(df_result)

        with timed("persist df_result"):
            df_result = df_result.persist()
            df_result.count()  # force la matérialisation pour un timing fiable

        # ====================================================================
        # ANALYSES PAS-À-PAS (étapes appelées après le matching).
        # Sorties séparées, à vérifier une à une avant agrégation ultérieure
        # dans un pipeline global. Univers PM/chute = matchés + récupérés N+1 ;
        # obs tardives IT, MRM_MISSING et CPT_ONLY exclus des comparaisons.
        # ====================================================================

        # ÉTAPE 0 — vue d'ensemble + taux distincts (matching vs récupération).
        with timed("ÉTAPE 0 synthèse"):
            print_synthese(df_result)

        # ÉTAPE 1 — diagnostic du fan-out (écart MRM clean ↔ synthèse mrm_nb).
        with timed("ÉTAPE 1 diagnostic fan-out"):
            diagnose_mrm_fanout(df_result, mrm_clean).show(30, truncate=False)

        # ÉTAPE 2 — restitution console de toutes les analyses (clause taguée) :
        # suivi consignes, taux de chute, consignes×PM, à supprimer non suivies,
        # provisionnement, ventilation CPT_ONLY, obs tardives (clos avant N+1).
        with timed("ÉTAPE 2 restitution analyses"):
            restituer_analyses(df_result)

            # Répartition CPT_ONLY par tag (segmentation actionnable des anomalies).
            print("\n[CPT_ONLY] répartition par tag :")
            (df_result.filter(F.col("TYPE_RECONCILIATION") == "CPT_ONLY")
                      .groupBy("TAG_CPT_ONLY")
                      .agg(F.count("*").alias("NB_DOSSIERS"),
                           F.round(F.sum("CPT_PM"), 2).alias("PM_CPT_TOTAL"))
                      .orderBy(F.desc("PM_CPT_TOTAL"))
                      .show(truncate=False))

        # ÉTAPE 3 — export multi-format sur DBFS (piloté par profile.py).
        if EXPORT_ANALYSES:
            with timed("ÉTAPE 3 export analyses"):
                export_analyses(
                    df_result,
                    formats      = EXPORT_FORMATS,
                    delta_schema = EXPORT_DELTA_SCHEMA,
                )
    return df_result


if __name__ == "__main__":
    spark = SparkSession.builder.appName("itip_fiab").getOrCreate()
    # AQE + skew join : critique pour les theta-joins des étapes windowed
    # (key + |datediff| <= N) qui sinon partent en shuffle déséquilibré.
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    run(spark)
