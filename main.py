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
    EXPORT_ANALYSES, EXPORT_FORMATS, EXPORT_DELTA_SCHEMA, EXPORT_GRAPHS,
    RECUP_NON_LABEL, CHECKPOINT_DIR,
)
from modules.load_data import load_cpt_raw, load_mrm_raw
from modules.transform import clean_cpt, clean_mrm
from modules.matching import matching_waterfall, recover_late_declarations
from modules.analysis import (
    flag_late_it_observations,
    enrich_result_tags,
    diagnose_mrm_fanout,
    restituer_analyses,
    export_analyses,
    restituer_graphiques,
)
from modules.kpi_export import print_synthese
from modules._timing import timed
import pyspark.sql.functions as F


def _split_mrm_statut(mrm_clean: DataFrame, statut_col: str = "MRM_STATUT_INV"):
    """Scinde le MRM clean en (OUI + statut absent, NON).

    OUI alimente le matching principal ; NON est réservé à la passe de repêchage
    des CPT_ONLY (PM MRM = 0). null traité comme non-NON (→ OUI)."""
    if statut_col not in mrm_clean.columns:
        return mrm_clean, mrm_clean.limit(0)
    is_non = F.coalesce(F.upper(F.trim(F.col(statut_col))) == F.lit("NON"), F.lit(False))
    return mrm_clean.filter(~is_non), mrm_clean.filter(is_non)


def run(spark: SparkSession) -> DataFrame:
    """Exécute le pipeline complet et affiche la synthèse client."""
    # Checkpoints fiables (DBFS) : les matérialisations du waterfall survivent
    # à la perte d'un executor (autoscaling). Sans checkpointDir, _materialize
    # retombe sur localCheckpoint → CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND possible
    # dès que le cluster réduit en cours de run.
    if CHECKPOINT_DIR:
        spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

    with timed("PIPELINE TOTAL"):
        cpt_clean = clean_cpt(load_cpt_raw(spark, db_cfg), tech_cfg)
        mrm_clean = clean_mrm(load_mrm_raw(spark, db_cfg), tech_cfg)

        # Le statut inventaire NON ne sert PAS au matching principal : il est
        # réservé au repêchage des CPT_ONLY (PM MRM = 0, non remonté à la
        # direction financière). On scinde : OUI (+ statut absent) pour le
        # matching, NON pour la passe de repêchage dédiée.
        mrm_oui, mrm_non = _split_mrm_statut(mrm_clean)

        df_result = matching_waterfall(cpt_clean, mrm_oui)

        # Déclarations tardives : CPT_ONLY retrouvés dans l'inventaire MRM N+1.
        # Les dossiers récupérés sont enrichis des infos MRM (TYPE_RECONCILIATION=
        # CPT_LATE). Cascade RECOVERY_KEYS : le waterfall principal rejoué dans
        # l'ordre (strict → flexible), mêmes règles et mêmes fenêtres.
        # L'étape gagnante est tracée dans LATE_KEY.
        if RUN_PARAMS.get("fichier_mrm_n1"):
            mrm_n1 = clean_mrm(load_mrm_raw(spark, db_cfg, "fichier_mrm_n1"), tech_cfg)
            # Même règle de statut que l'exercice N : seuls les OUI (+ statut
            # absent) du N+1 produisent des CPT_LATE — un NON du N+1 a une PM
            # MRM = 0, le prendre comme contrepartie fausserait l'univers de
            # chute (CPT_LATE est INCLUS dans les métriques). Les NON du N+1
            # sont écartés ; le repêchage statut NON porte sur l'exercice N.
            mrm_n1_oui, _ = _split_mrm_statut(mrm_n1)
            df_result = recover_late_declarations(df_result, [("MRM_N1", mrm_n1_oui)])

        # Repêchage via statut NON : les CPT_ONLY restants retrouvés dans les MRM
        # NON sont tagués CPT_RECUP_NON (LATE_SOURCE=STATUT_NON). Label distinct →
        # EXCLU de toutes les métriques, présenté dans une analyse dédiée. Les MRM
        # NON non repêchés ne sont jamais unionnés (ils disparaissent, zéro
        # empreinte dans la volumétrie). Même cascade RECOVERY_KEYS que le N+1.
        df_result = recover_late_declarations(
            df_result, [("STATUT_NON", mrm_non)], label=RECUP_NON_LABEL,
        )

        # Observations tardives IT : CPT_ONLY garantie 60 survenus en fin d'année,
        # absents du MRM courant et du N+1 → tagués CPT_OBS_TARDIVE (anomalie,
        # LATE_SOURCE=OBS_TARDIVE_IT). Jamais matchés → exclus des taux et des PM.
        df_result = flag_late_it_observations(df_result)

        # Colonnes persistantes : consigne reformatée (MRM_ACTION) + tag des
        # orphelins CPT_ONLY (TAG_CPT_ONLY).
        df_result = enrich_result_tags(df_result)

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

        # ÉTAPE 1 — diagnostic du fan-out (écart MRM ↔ synthèse mrm_nb). Univers =
        # MRM OUI (celui du matching principal ; les NON ne sont pas comparés ici).
        with timed("ÉTAPE 1 diagnostic fan-out"):
            diagnose_mrm_fanout(df_result, mrm_oui).show(30, truncate=False)

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

        # ÉTAPE 4 — graphiques de restitution (titres-messages : justification
        # du compte, couverture des listes d'arrêts, chute par clause/consigne,
        # conformité des consignes, anomalies). Affichés en notebook + PNG DBFS.
        if EXPORT_GRAPHS:
            with timed("ÉTAPE 4 graphiques"):
                restituer_graphiques(df_result)
    return df_result


if __name__ == "__main__":
    spark = SparkSession.builder.appName("itip_fiab").getOrCreate()
    # AQE + skew join : critique pour les theta-joins des étapes windowed
    # (key + |datediff| <= N) qui sinon partent en shuffle déséquilibré.
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    run(spark)
