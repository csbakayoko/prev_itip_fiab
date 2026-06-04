"""
Pipeline de fiabilisation ITIP-FIAB — point d'entrée.

Spine essentiel, mono-client (SODEXO) :
    load → clean → waterfall → synthèse SODEXO (ASCII console)

Périmètre piloté par config/profile.py. Lancement :
    spark-submit main.py        (ou exécution dans un notebook Databricks)
"""

import logging

from pyspark.sql import DataFrame, SparkSession

from config import db_cfg, tech_cfg, RUN_PARAMS
from modules.load_data import load_cpt_raw, load_mrm_raw
from modules.transform import clean_cpt, clean_mrm
from modules.matching import matching_waterfall, recover_late_declarations
from modules.analysis import (
    ventilate_cpt_only,
    flag_late_it_observations,
    enrich_result_tags,
    study_provisionnement,
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
        # absents du MRM courant et du N+1 → calés en CPT_LATE (LATE_SOURCE=OBS_TARDIVE_IT).
        df_result = flag_late_it_observations(df_result)

        # Colonnes persistantes : consigne reformatée (MRM_ACTION) + tag des
        # orphelins CPT_ONLY (TAG_CPT_ONLY).
        df_result = enrich_result_tags(df_result)

        with timed("persist df_result"):
            df_result = df_result.persist()
            df_result.count()  # force la matérialisation pour un timing fiable

        print_synthese(df_result)

        # Ventilation des CPT_ONLY définitifs par survenance × garantie,
        # PM décroissant — où se concentre la PM non réconciliée.
        with timed("ventilation CPT_ONLY"):
            print("\n[CPT_ONLY] ventilation par survenance × garantie (PM décroissant) :")
            ventilate_cpt_only(df_result).show(50, truncate=False)

            print("\n[CPT_ONLY] répartition par tag :")
            (df_result.filter(F.col("TYPE_RECONCILIATION") == "CPT_ONLY")
                      .groupBy("TAG_CPT_ONLY")
                      .agg(F.count("*").alias("NB_DOSSIERS"),
                           F.round(F.sum("CPT_PM"), 2).alias("PM_CPT_TOTAL"))
                      .orderBy(F.desc("PM_CPT_TOTAL"))
                      .show(truncate=False))

        # Étude du provisionnement (matchés hors à supprimer) : sur/sous/conforme.
        with timed("étude provisionnement"):
            print("\n[PROVISIONNEMENT] sur/sous/conforme (matchés hors à supprimer) :")
            study_provisionnement(df_result).show(truncate=False)
    return df_result


if __name__ == "__main__":
    spark = SparkSession.builder.appName("itip_fiab").getOrCreate()
    # AQE + skew join : critique pour les theta-joins des étapes windowed
    # (key + |datediff| <= N) qui sinon partent en shuffle déséquilibré.
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    run(spark)
