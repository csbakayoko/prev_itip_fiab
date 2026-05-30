"""
Pipeline de fiabilisation ITIP-FIAB — point d'entrée.

Spine essentiel, mono-client (SODEXO) :
    load → clean → waterfall → synthèse SODEXO (ASCII console)

Périmètre piloté par config/profile.py. Lancement :
    spark-submit main.py        (ou exécution dans un notebook Databricks)
"""

import logging

# ── Logging ─────────────────────────────────────────────────────────────────
# Racine en WARNING (silence le bruit socket de py4j / Spark Connect, qui sinon
# inonde la sortie de ConnectionResetError non-fatals) ; nos packages en INFO.
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
logging.getLogger("config").setLevel(logging.INFO)
logging.getLogger("modules").setLevel(logging.INFO)
for _noisy in ("py4j", "py4j.clientserver", "py4j.java_gateway"):
    logging.getLogger(_noisy).setLevel(logging.ERROR)

from pyspark.sql import DataFrame, SparkSession

from config import db_cfg, tech_cfg, RUN_PARAMS
from modules.load_data import load_cpt_raw, load_mrm_raw
from modules.transform import clean_cpt, clean_mrm
from modules.matching import matching_waterfall, recover_late_declarations
from modules.kpi_export import print_synthese


def run(spark: SparkSession) -> DataFrame:
    """Exécute le pipeline complet et affiche la synthèse client."""
    cpt_clean = clean_cpt(load_cpt_raw(spark, db_cfg), tech_cfg)
    mrm_clean = clean_mrm(load_mrm_raw(spark, db_cfg), tech_cfg)

    df_result = matching_waterfall(cpt_clean, mrm_clean)

    # Déclarations tardives : CPT_ONLY retrouvés dans les MRM postérieurs (N+1
    # puis N+2), en cascade. Les dossiers récupérés sont enrichis des infos MRM.
    inventories = []
    for tag, path_key in (("MRM_N1", "fichier_mrm_n1"), ("MRM_N2", "fichier_mrm_n2")):
        if RUN_PARAMS.get(path_key):
            inventories.append((tag, clean_mrm(load_mrm_raw(spark, db_cfg, path_key), tech_cfg)))
    if inventories:
        df_result = recover_late_declarations(df_result, inventories)

    df_result = df_result.persist()
    print_synthese(df_result)
    return df_result


if __name__ == "__main__":
    spark = SparkSession.builder.appName("itip_fiab").getOrCreate()
    run(spark)
