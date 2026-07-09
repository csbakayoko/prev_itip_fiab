"""
Écriture Delta historisée — détail du backtesting et briques partagées.

Les tables du projet sont HISTORISÉES par run : partitionnées par
DATE_INVENTAIRE et écrites en replaceWhere sur (DATE_INVENTAIRE, PERIMETRE) —
rejouer un run remplace exactement SES lignes, un nouvel inventaire ou un
autre périmètre s'ajoute (2023 et 2024 coexistent). Les noms de tables sont
STABLES (le périmètre est une colonne, pas un suffixe de nom) : Power BI
(SQL Warehouse) filtre et compare les inventaires nativement.

- save_result_delta      : df_result (une ligne = un dossier du run) →
  <schema>.resultat_backtest, pour les analyses fines au-delà des tables
  métriques agrégées (metrique_*) ;
- write_delta_historise / to_date_iso : briques réutilisées par l'export des
  métriques (core.metrics.export).
"""

import logging
from datetime import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from config import CLIENT_NAME, EXPORT_RESULT_TABLE, PERIMETRE_LABEL

logger = logging.getLogger(__name__)


def to_date_iso(date_inventaire: str, strict: bool = True) -> Optional[str]:
    """Convertit 'dd/MM/yyyy' → 'yyyy-MM-dd' (clé d'historisation).

    Args:
        date_inventaire : date du run ("31/12/2023"). "n/d"/None = non résolue.
        strict          : True = lève ValueError si la date n'est pas résoluble
                          (on refuse d'historiser à l'aveugle) ; False = None.
    """
    try:
        return datetime.strptime(date_inventaire, "%d/%m/%Y").date().isoformat()
    except (TypeError, ValueError):
        if strict:
            raise ValueError(
                f"date_inventaire invalide ({date_inventaire!r}) — attendu 'dd/MM/yyyy', "
                "impossible d'historiser le run."
            )
        return None


def write_delta_historise(
    df: DataFrame, table: str, date_iso: str, perimetre: str = PERIMETRE_LABEL,
) -> str:
    """Écrit df en table Delta partitionnée par DATE_INVENTAIRE, historisée
    par run : replaceWhere sur (DATE_INVENTAIRE, PERIMETRE).

    Seules les lignes du run (même date, même périmètre) sont remplacées ; le
    schéma metastore est créé s'il n'existe pas. `df` doit porter les colonnes
    DATE_INVENTAIRE (date) et PERIMETRE.

    Returns:
        Nom complet de la table écrite.
    """
    spark = df.sparkSession
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    (
        df.write.format("delta")
          .mode("overwrite")
          .option("replaceWhere",
                  f"DATE_INVENTAIRE = '{date_iso}' AND PERIMETRE = '{perimetre}'")
          .partitionBy("DATE_INVENTAIRE")
          .saveAsTable(table)
    )
    logger.info("Delta → %s (run %s / %s remplacé)", table, date_iso, perimetre)
    return table


def save_result_delta(
    df_result      : DataFrame,
    delta_schema   : str,
    date_inventaire: str,
    table_name     : str = EXPORT_RESULT_TABLE,
) -> str:
    """
    Écrit df_result en table Delta <delta_schema>.<table_name>.

    Colonnes de run ajoutées (schéma standard) : DATE_INVENTAIRE (date,
    partition), PERIMETRE (clé d'historisation avec la date), LIBELLE_RUN
    (libellé du run) et TS_RUN (horodatage d'écriture).

    Args:
        df_result       : résultat du pipeline (main.build_df_result).
        delta_schema    : schéma metastore cible (créé s'il n'existe pas).
        date_inventaire : date du run au format "dd/MM/yyyy" — les lignes
                          (date, périmètre) du run sont remplacées, les autres
                          inventaires/périmètres sont préservés.
        table_name      : nom de la table (défaut : config.EXPORT_RESULT_TABLE).

    Returns:
        Nom complet de la table écrite (schéma.table).

    Raises:
        ValueError si delta_schema est vide ou si date_inventaire n'est pas
        une date "dd/MM/yyyy" (ex. "n/d") — on refuse d'historiser à l'aveugle.
    """
    if not delta_schema:
        raise ValueError("delta_schema vide — pas de cible Delta pour df_result.")
    date_iso = to_date_iso(date_inventaire)

    df = (
        df_result
        .withColumn("DATE_INVENTAIRE", F.lit(date_iso).cast("date"))
        .withColumn("PERIMETRE",       F.lit(PERIMETRE_LABEL))
        .withColumn("LIBELLE_RUN",     F.lit(CLIENT_NAME))
        .withColumn("TS_RUN",          F.current_timestamp())
    )
    return write_delta_historise(df, f"{delta_schema}.{table_name}", date_iso)
