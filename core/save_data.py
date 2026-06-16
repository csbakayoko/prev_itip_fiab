"""
Export de DataFrames SPARK — fichiers (excel / json / csv / parquet / delta) & tables metastore.

Complément de core/metrics.export_metriques (qui exporte les TABLES MÉTRIQUES
*pandas*, Excel prioritaire). Ici on exporte des DataFrames *Spark* bruts
(df_result, CPT/MRM nettoyés, toute table volumineuse), pour :
    - **stocker dans le metastore Databricks** (hive_metastore.<schema>.<table>) →
      lisible par Power BI (SQL Warehouse) et réutilisable par d'autres jobs ;
    - archiver en parquet / csv sur DBFS / ADLS ;
    - un export Excel d'appoint (petits volumes uniquement, cf. EXCEL_MAX_ROWS).

Aucune dépendance à la config : tout passe en argument → réutilisable tel quel
dans un Job Databricks. Excel reste le format privilégié pour la restitution,
mais le reste compte (parquet pour la volumétrie, table Delta pour Power BI).
"""

import logging
import os
from typing import Dict, Iterable, List

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

# Excel charge tout en mémoire driver (toPandas) : garde-fou de volumétrie.
EXCEL_MAX_ROWS = 100_000

# Formats fichier (≠ table metastore, traitée à part).
_FILE_FORMATS = {"excel", "json", "csv", "parquet", "delta"}


# ============================================================================
# WRITERS BAS NIVEAU
# ============================================================================

def save_to_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """
    Écrit `df` comme table Delta dans le metastore (hive_metastore.<schema>.<table>).

    - mode="overwrite" : snapshot, le schéma peut évoluer (overwriteSchema).
    - mode="append"    : historisation, fusion de schéma tolérée (mergeSchema).

    C'est LA porte d'entrée « stocker dans le hive_metastore » : la table est
    ensuite lisible en SQL (`SELECT * FROM <table_name>`) et par Power BI via le
    connecteur Azure Databricks (SQL Warehouse).
    """
    writer = df.write.format("delta").mode(mode)
    writer = (writer.option("overwriteSchema", "true") if mode == "overwrite"
              else writer.option("mergeSchema", "true"))
    writer.saveAsTable(table_name)
    logger.info("Table metastore écrite [%s] mode=%s", table_name, mode)


def save_to_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Écrit `df` en Parquet (DBFS / ADLS). Robuste pour la volumétrie."""
    df.write.mode(mode).parquet(path)
    logger.info("Parquet écrit [%s] mode=%s", path, mode)


def save_to_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Écrit `df` en Delta sur un chemin (table externe, non managée)."""
    df.write.format("delta").mode(mode).save(path)
    logger.info("Delta (chemin) écrit [%s] mode=%s", path, mode)


def save_to_csv(df: DataFrame, path: str, mode: str = "overwrite", delimiter: str = ";") -> None:
    """Écrit `df` en CSV (header, UTF-8, séparateur paramétrable)."""
    (df.write.mode(mode)
       .option("header", "true").option("delimiter", delimiter).option("encoding", "UTF-8")
       .csv(path))
    logger.info("CSV écrit [%s] mode=%s", path, mode)


def save_to_json(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Écrit `df` en JSON (writer Spark → dossier de NDJSON, 1 objet par ligne).

    Adapté à la volumétrie (écriture distribuée). Pour un fichier JSON unique et
    compact (orient=records), les TABLES MÉTRIQUES passent par metrics.export_metriques
    (pandas) ; ici on reste côté Spark."""
    df.write.mode(mode).json(path)
    logger.info("JSON écrit [%s] mode=%s", path, mode)


# ============================================================================
# EXPORT GÉNÉRIQUE D'UN DATAFRAME
# ============================================================================

def export_dataframe(
    df        : DataFrame,
    dest      : str,
    fmt       : str = "parquet",
    mode      : str = "overwrite",
    delimiter : str = ";",
    sheet_name: str = "Export",
) -> None:
    """
    Exporte un DataFrame Spark dans le format choisi.

    Args:
        df   : DataFrame Spark.
        dest : chemin DBFS/ADLS (excel/json/csv/parquet/delta) OU nom de table (fmt="table").
        fmt  : "excel" | "json" | "csv" | "parquet" | "delta" | "table".
        mode : "overwrite" | "append".
    """
    fmt = fmt.lower()
    if fmt == "table":
        save_to_table(df, dest, mode=mode)
    elif fmt == "parquet":
        save_to_parquet(df, dest, mode=mode)
    elif fmt == "delta":
        save_to_delta(df, dest, mode=mode)
    elif fmt == "csv":
        save_to_csv(df, dest, mode=mode, delimiter=delimiter)
    elif fmt == "json":
        save_to_json(df, dest, mode=mode)
    elif fmt == "excel":
        _to_excel_single(df, dest, sheet_name=sheet_name)
    else:
        raise ValueError(
            f"Format inconnu : '{fmt}'. Choisir : excel, json, csv, parquet, delta, table."
        )


def _to_excel_single(df: DataFrame, path: str, sheet_name: str = "Export") -> None:
    """Excel d'appoint d'un Spark DF (toPandas, garde-fou volumétrie)."""
    nb = df.count()
    if nb > EXCEL_MAX_ROWS:
        logger.warning("Export Excel ignoré [%s] : %d lignes > %d — préférer parquet/csv.",
                       path, nb, EXCEL_MAX_ROWS)
        return
    _ensure_parent(path)
    df.toPandas().to_excel(path, index=False, sheet_name=sheet_name[:31])
    logger.info("Excel écrit [%s] (%d lignes)", path, nb)


# ============================================================================
# EXPORT D'UN ENSEMBLE DE DATAFRAMES SPARK
# ============================================================================

def export_spark_tables(
    tables    : Dict[str, DataFrame],
    base_path : str = None,
    formats   : Iterable[str] = ("parquet",),
    schema    : str = None,
    mode      : str = "overwrite",
    excel_path: str = None,
) -> None:
    """
    Exporte un dict {nom: Spark DataFrame} en un ou plusieurs formats.

    - Formats fichier (json/csv/parquet/delta) → `<base_path>/<nom>_<fmt>` (requiert base_path).
    - "excel"  → un classeur multi-onglets unique (`excel_path` ou <base_path>/tables.xlsx).
    - "table"  → tables Delta metastore `<schema>.<nom>` (requiert schema) — stockage
                 hive_metastore, le plus pratique pour Power BI via SQL Warehouse.

    Tout échec sur une (table, format) est loggé et n'interrompt pas le reste.
    """
    formats = [f.lower() for f in formats]
    print(f"[save] export {list(tables)} → {formats} "
          f"(base={base_path or '—'}, schema={schema or '—'})")

    # Excel : un seul classeur multi-onglets pour tout le dict.
    if "excel" in formats:
        path = excel_path or (f"{base_path.rstrip('/')}/tables.xlsx" if base_path else None)
        if path:
            _to_excel_multi(tables, path)
        else:
            logger.warning("Excel demandé mais ni excel_path ni base_path fourni — ignoré.")

    for name, df in tables.items():
        for fmt in formats:
            try:
                if fmt == "excel":
                    continue                                  # déjà traité (classeur unique)
                elif fmt == "table":
                    if not schema:
                        logger.warning("Format 'table' demandé sans schema — '%s' ignoré.", name)
                        continue
                    save_to_table(df, f"{schema}.{name}", mode=mode)
                elif fmt in _FILE_FORMATS:
                    if not base_path:
                        logger.warning("Format '%s' demandé sans base_path — '%s' ignoré.", fmt, name)
                        continue
                    export_dataframe(df, f"{base_path.rstrip('/')}/{name}_{fmt}", fmt=fmt, mode=mode)
                else:
                    logger.warning("Format inconnu '%s' — '%s' ignoré.", fmt, name)
            except Exception as exc:
                logger.error("Échec export %s/%s : %s", name, fmt, exc)
    print("[save] export terminé")


def _to_excel_multi(tables: Dict[str, DataFrame], path: str) -> None:
    """Classeur Excel multi-onglets de Spark DFs (un onglet par table, garde-fou)."""
    import pandas as pd
    _ensure_parent(path)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in tables.items():
            nb = df.count()
            if nb > EXCEL_MAX_ROWS:
                logger.warning("Onglet '%s' ignoré : %d lignes > %d.", name, nb, EXCEL_MAX_ROWS)
                continue
            df.toPandas().to_excel(writer, sheet_name=name[:31], index=False)
            print(f"  ✓ onglet '{name[:31]}' ({nb:,} lignes)")
    logger.info("Classeur Excel écrit [%s]", path)


def _ensure_parent(path: str) -> None:
    """Crée le dossier parent pour les writers locaux/pandas (chemins /dbfs/...)."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
