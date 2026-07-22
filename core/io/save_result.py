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
- write_delta_historise / to_date_iso / cle_run : briques réutilisées par
  l'export des métriques (core.metrics.export).
"""

import logging
from datetime import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from config import CLIENT_NAME, EXPORT_RESULT_TABLE, PERIMETRE_LABEL

logger = logging.getLogger(__name__)


def cle_run(date_inventaire: str, perimetre: str = PERIMETRE_LABEL) -> str:
    """Clé de liaison du run : « <date>|<périmètre> » (ex. « 2023-12-31|MULTI »).

    Posée sur TOUTES les tables exportées (métriques + resultat_backtest) :
    c'est la colonne de relation du modèle en étoile Power BI — une seule
    colonne relie dim_run à chaque table, sans clé composite à fabriquer
    côté rapport. Même identité que l'historisation Delta (DATE_INVENTAIRE ×
    PERIMETRE) : une clé = un run.
    """
    return f"{date_inventaire}|{perimetre}"


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
    try:
        (
            df.write.format("delta")
              .mode("overwrite")
              .option("replaceWhere",
                      f"DATE_INVENTAIRE = '{date_iso}' AND PERIMETRE = '{perimetre}'")
              .partitionBy("DATE_INVENTAIRE")
              .saveAsTable(table)
        )
    except Exception as exc:
        # replaceWhere interdit tout changement de schéma : si la table existante
        # date d'une version antérieure des exports, Delta refuse d'écrire. On
        # nomme la table ET le remède — sinon l'export échoue au milieu de la
        # boucle avec une AnalysisException illisible.
        msg = str(exc)
        if "schema" in msg.lower() or "replaceWhere" in msg or "not enough data columns" in msg:
            raise RuntimeError(
                f"Écriture Delta refusée pour {table} : la table existante a un "
                "schéma différent des données du run (l'historisation replaceWhere "
                "interdit tout changement de schéma). Remède, UNE FOIS : "
                f"`DROP TABLE IF EXISTS {table}` — ou relancer le notebook "
                "itip_fiab_powerbi avec le widget reinitialiser_tables = 'oui', "
                "qui purge toutes les tables du schéma cible avant l'export. "
                "La table se recrée ensuite automatiquement."
            ) from exc
        raise
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
    (libellé du run), CLE_RUN (clé de liaison du modèle en étoile Power BI —
    la même que sur les tables métriques) et TS_RUN (horodatage d'écriture).

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
        .withColumn("CLE_RUN",         F.lit(cle_run(date_iso)))
        .withColumn("TS_RUN",          F.current_timestamp())
    )
    return write_delta_historise(df, f"{delta_schema}.{table_name}", date_iso)
