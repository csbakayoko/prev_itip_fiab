"""
Écriture du détail du backtesting (df_result) en table Delta historisée.

Une ligne df_result = un dossier du run (réconcilié, récupéré ou orphelin).
La table est HISTORISÉE par date d'inventaire : rejouer un inventaire remplace
SES lignes (replaceWhere sur la partition), un nouvel inventaire s'ajoute —
2023 et 2024 coexistent. Power BI (SQL Warehouse) y fait les analyses fines,
au-delà des tables métriques agrégées (itip_metric_*).
"""

import logging
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from config import CLIENT_NAME, EXPORT_RESULT_TABLE

logger = logging.getLogger(__name__)


def save_result_delta(
    df_result      : DataFrame,
    delta_schema   : str,
    date_inventaire: str,
    table_name     : str = EXPORT_RESULT_TABLE,
) -> str:
    """
    Écrit df_result en table Delta <delta_schema>.<table_name>.

    Colonnes de run ajoutées : DATE_INVENTAIRE (date, partition),
    PERIMETRE (libellé du run) et TS_RUN (horodatage d'écriture).

    Args:
        df_result       : résultat du pipeline (main.build_df_result).
        delta_schema    : schéma metastore cible (créé s'il n'existe pas).
        date_inventaire : date du run au format "dd/MM/yyyy" — sa partition
                          est remplacée, les autres inventaires sont préservés.
        table_name      : nom de la table (défaut : config.EXPORT_RESULT_TABLE).

    Returns:
        Nom complet de la table écrite (schéma.table).

    Raises:
        ValueError si delta_schema est vide ou si date_inventaire n'est pas
        une date "dd/MM/yyyy" (ex. "n/d") — on refuse d'historiser à l'aveugle.
    """
    if not delta_schema:
        raise ValueError("delta_schema vide — pas de cible Delta pour df_result.")
    try:
        date_iso = datetime.strptime(date_inventaire, "%d/%m/%Y").date().isoformat()
    except (TypeError, ValueError):
        raise ValueError(
            f"date_inventaire invalide ({date_inventaire!r}) — attendu 'dd/MM/yyyy', "
            "impossible d'historiser le run."
        )

    table = f"{delta_schema}.{table_name}"
    df = (
        df_result
        .withColumn("DATE_INVENTAIRE", F.lit(date_iso).cast("date"))
        .withColumn("PERIMETRE",       F.lit(CLIENT_NAME))
        .withColumn("TS_RUN",          F.current_timestamp())
    )

    spark = df_result.sparkSession
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {delta_schema}")
    (
        df.write.format("delta")
          .mode("overwrite")
          .option("replaceWhere", f"DATE_INVENTAIRE = '{date_iso}'")
          .partitionBy("DATE_INVENTAIRE")
          .saveAsTable(table)
    )
    logger.info("df_result → %s (partition %s remplacée)", table, date_iso)
    return table
