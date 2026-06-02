"""
Orchestration des investigations orphelins — point d'entrée.

Enchaîne : pipeline principal → orphelins → entrepôt → traçage → stats, pour
les bases dont un fichier entrepôt est configuré (FICHIER_ENTREPOT_CPT / _MRM).

Lancement :
    spark-submit investigations/run.py     (ou import depuis un notebook)
"""

from pyspark.sql import DataFrame, SparkSession

from config import RUN_PARAMS
from modules._timing import timed
from investigations.orphans import extract_orphans
from investigations.warehouse import load_warehouse_excel, prepare_warehouse
from investigations.analyze import trace_history, history_stats, print_summary

# base → clé RUN_PARAMS du fichier entrepôt
_ENTREPOT_KEY = {"cpt": "entrepot_cpt", "mrm": "entrepot_mrm"}


def investigate_base(df_result: DataFrame, spark: SparkSession, base: str) -> dict:
    """Trace les orphelins d'une base contre son entrepôt. {} si pas de fichier."""
    path = RUN_PARAMS.get(_ENTREPOT_KEY[base])
    if not path:
        print(f"[investig:{base}] aucun fichier entrepôt configuré — ignoré.")
        return {}

    with timed(f"investigate {base}"):
        orphans   = extract_orphans(df_result, base)
        warehouse = prepare_warehouse(load_warehouse_excel(spark, path), base)
        traced    = trace_history(orphans, warehouse).cache()
        stats     = history_stats(traced, base).cache()
        stats.count()  # matérialise pour un timing fiable
        print_summary(stats, base)

    return {f"{base}_traced": traced, f"{base}_stats": stats}


def investigate(spark: SparkSession, df_result: DataFrame = None) -> dict:
    """
    Lance les investigations pour CPT et MRM.

    Args:
        spark     : SparkSession active.
        df_result : résultat de réconciliation déjà calculé. Si None, le pipeline
                    principal (main.run) est exécuté pour le produire.

    Returns:
        Dict {f"{base}_traced", f"{base}_stats"} pour chaque base investiguée.
    """
    if df_result is None:
        from main import run
        df_result = run(spark)

    results: dict = {}
    for base in ("cpt", "mrm"):
        results.update(investigate_base(df_result, spark, base))
    return results


if __name__ == "__main__":
    spark = SparkSession.builder.appName("itip_fiab_investigations").getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    investigate(spark)
