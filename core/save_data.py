"""
Export des résultats vers Power BI, CSV, Excel, Parquet, Delta.

Formats supportés :
    - delta   → table Delta metastore (Power BI direct connect)
    - csv     → fichier CSV DBFS / ADLS
    - excel   → fichier .xlsx via pandas (< 100k lignes)
    - parquet → fichier Parquet DBFS / ADLS
"""

import logging
import os
import pandas as pd
from typing import Dict, List

from pyspark.sql import DataFrame

from config import (
    ExportFormat,
    ExportConfig,
    out_cfg,
    export_cfg,
)

logger = logging.getLogger(__name__)


# ============================================================================
# SAUVEGARDE INTERNE (bas niveau)
# ============================================================================

def save_to_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Sauvegarde en Delta sur un chemin DBFS (usage interne)."""
    df.write.format("delta").mode(mode).save(path)


def save_to_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Sauvegarde en Parquet (usage interne ou export ADLS)."""
    df.write.mode(mode).parquet(path)


def save_to_table(
    df        : DataFrame,
    table_name: str,
    mode      : str = "overwrite",
) -> None:
    """Sauvegarde comme table Delta dans le metastore Databricks.

    - mode="overwrite" : overwriteSchema=true (snapshot, schéma libre d'évoluer)
    - mode="append"    : pas d'overwriteSchema (compat schéma requise)
    """
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    else:
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(table_name)
    logger.info(f"{table_name} sauvegardée [{mode}]")


# ============================================================================
# EXPORT GÉNÉRIQUE (CSV / Excel / Parquet / Delta)
# ============================================================================

def export_dataframe(
    df        : DataFrame,
    path      : str,
    fmt       : ExportFormat = "csv",
    mode      : str          = "overwrite",
    delimiter : str          = ";",
    sheet_name: str          = "Export",
) -> None:
    """
    Exporte un DataFrame Spark dans le format choisi.

    Args:
        df         : DataFrame Spark à exporter
        path       : chemin de destination (DBFS ou local driver)
        fmt        : "delta" | "csv" | "excel" | "parquet"
        mode       : "overwrite" | "append"
        delimiter  : séparateur CSV (défaut ";")
        sheet_name : onglet Excel
    """
    fmt = fmt.lower()

    if fmt == "delta":
        df.write.format("delta").mode(mode).save(path)

    elif fmt == "csv":
        (
            df.write
              .mode(mode)
              .option("header",    "true")
              .option("delimiter", delimiter)
              .option("encoding",  "UTF-8")
              .csv(path)
        )

    elif fmt == "parquet":
        df.write.mode(mode).parquet(path)

    elif fmt == "excel":
        nb = df.count()
        if nb > 100_000:
            logger.warning(
                f"Export Excel ignoré : {nb:,} lignes > 100 000. "
                "Utiliser CSV ou Parquet pour les gros volumes."
            )
            return
        pdf = df.toPandas()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pdf.to_excel(path, index=False, sheet_name=sheet_name)

    else:
        raise ValueError(
            f"Format inconnu : '{fmt}'. Choisir parmi : delta, csv, excel, parquet."
        )

    logger.info(f"Export {fmt.upper()} → {path}")


# ============================================================================
# EXPORT POWER BI (tables Delta metastore)
# ============================================================================

# Tables écrites en append (historisation) au lieu d'overwrite (snapshot).
# synthese_client conserve une trace de chaque run pour comparer les périodes.
APPEND_TABLES = {"synthese_client"}


def export_for_powerbi(results: Dict[str, DataFrame]) -> None:
    """
    Écrit toutes les tables agrégées dans le schéma Power BI.

    Mode d'écriture par table :
        - synthese_client → append (historisation des runs)
        - autres          → overwrite (snapshot du dernier run)

    Args:
        results : Dict {clé_logique: DataFrame} produit par run().
    """
    table_map = out_cfg.as_table_map()
    print(f"\nExport Power BI ({len(results)} tables)...")

    for key, df in results.items():
        if key not in table_map:
            logger.warning(f"Clé '{key}' absente du TABLE_MAP — ignorée")
            continue
        mode = "append" if key in APPEND_TABLES else "overwrite"
        save_to_table(df, table_map[key], mode=mode)
        print(f"  ✓ [{mode}] {table_map[key]}")

    print(f"Export terminé → schéma '{out_cfg.schema}'\n")


# ============================================================================
# EXPORT MULTI-FORMAT (livraison externe / archivage / debug)
# ============================================================================

def export_all_formats(
    results   : Dict[str, DataFrame],
    cfg       : ExportConfig = export_cfg,  # ← piloté par settings.py
) -> None:
    """
    Exporte toutes les tables dans un ou plusieurs formats fichier.

    Utile pour livraison externe, archivage ou debug.

    Args:
        results : Dict {clé_logique: DataFrame}
        cfg     : ExportConfig (base_path, formats, delimiter)

    Exemple:
        # Avec les defaults de settings.py
        export_all_formats(results)

        # Override ponctuel
        export_all_formats(results, ExportConfig(
            base_path = "dbfs:/FileStore/exports/livraison",
            formats   = ("csv", "excel"),
        ))
    """
    print(f"\nExport multi-format → {cfg.base_path} {list(cfg.formats)}")

    for key, df in results.items():
        for fmt in cfg.formats:
            ext  = "xlsx" if fmt == "excel" else fmt
            path = (
                f"{cfg.base_path}/{key}.{ext}"
                if fmt == "excel"
                else f"{cfg.base_path}/{key}_{fmt}"
            )
            try:
                export_dataframe(df, path, fmt=fmt, delimiter=cfg.delimiter)
                print(f"  ✓ [{fmt.upper()}] {path}")
            except Exception as e:
                logger.error(f"Échec export {fmt.upper()} pour '{key}' : {e}")

    print("Export multi-format terminé\n")


def export_to_excel(
    tables     : Dict[str, DataFrame],
    output_path: str = "/Workspace/Users/cheickseko.bakayoko@axa.fr/itip_fiab/outputs/itip_fiab_report.xlsx",
) -> str:
    """
    Exporte toutes les tables dans un seul fichier Excel multi-onglets.

    Chaque clé du dict → un onglet dans le fichier.

    Args:
        tables      : Dict {nom_table: DataFrame Spark}
        output_path : Chemin DBFS ou Azure Blob

    Returns:
        Chemin du fichier produit
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    MAX_ROWS_EXCEL = 100_000

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for table_name, df_spark in tables.items():

            nb = df_spark.count()
            if nb > MAX_ROWS_EXCEL:
                logger.warning(
                    f"Onglet '{table_name}' ignoré : {nb:,} lignes > {MAX_ROWS_EXCEL:,}. "
                    "Utiliser CSV ou Parquet pour les gros volumes."
                )
                continue

            # Nom onglet : max 31 chars (limite Excel)
            sheet_name = table_name[:31]

            df_spark.toPandas().to_excel(
                writer,
                sheet_name = sheet_name,
                index      = False,
            )
            print(f"  ✓ Onglet '{sheet_name}' — {nb:,} lignes")

    print(f"\n  Fichier exporté : {output_path}")
    return output_path


def export_to_parquet(
    tables    : Dict[str, DataFrame],
    output_dir: str = "/dbfs/mnt/exports/itip_fiab/",
) -> str:
    """
    Exporte chaque table en Parquet dans un dossier commun.

    Structure produite :
        itip_fiab/
            ├── synthese_consignes.parquet
            ├── provisionnement.parquet
            ├── ecarts_tranches.parquet
            └── ...

    Args:
        tables     : Dict {nom_table: DataFrame Spark}
        output_dir : Dossier racine DBFS ou Azure Blob

    Returns:
        Chemin du dossier produit
    """
    os.makedirs(output_dir, exist_ok=True)

    for table_name, df_spark in tables.items():
        path = os.path.join(output_dir, f"{table_name}.parquet")
        df_spark.coalesce(1).write.mode("overwrite").parquet(path)
        print(f"  ✓ {table_name}.parquet — {df_spark.count():,} lignes")

    print(f"\n  Dossier exporté : {output_dir}")
    return output_dir

"""
# Dans run() — après les analyses
if export_files:
    export_to_excel(
        tables      = all_tables,
        output_path = "/dbfs/mnt/exports/itip_fiab_report.xlsx",
    )
"""
