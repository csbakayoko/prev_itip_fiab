"""
Restitution et export des résultats d'analyse.

- collect_analyses : assemble toutes les analyses en un dict {nom: DataFrame},
  chacune ventilée par CLAUSE / TYPE_CLAUSE (colonnes portées par les données).
- restituer_analyses : restitution console (.show de chaque analyse).
- export_analyses : écriture multi-format (CSV / Parquet / Excel multi-onglets /
  table Delta metastore), le périmètre apparaît dans le nom de fichier/table.

v2.0 multi-périmètre : aucune clause n'est figée. Chaque table est déjà
ventilée par (CLAUSE, TYPE_CLAUSE) ; le run porte un libellé de périmètre
(CLIENT_NAME) utilisé uniquement pour nommer les sorties.
"""

import logging
import os
from typing import Dict, Iterable, Optional

import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from config import CLIENT_NAME, CLIENT_CLAUSES
from modules.analysis.helpers import derive_clause_column

from modules.analysis.taux_chute import analyze_taux_chute
from modules.analysis.consignes import (
    analyze_suivi_consignes_global,
    analyze_consignes_pm,
    analyze_delete_non_suivies,
)
from modules.analysis.provisionnement import study_provisionnement
from modules.analysis.orphelins import ventilate_cpt_only, analyze_obs_tardives

logger = logging.getLogger(__name__)


# Libellé de périmètre pour nommer les sorties (fichiers / tables). En
# multi-périmètre il n'y a pas de clause unique : on note la clause si le run
# est filtré sur une seule, sinon "MULTI". La clause réelle reste DANS chaque
# table (colonne CLAUSE), jamais écrasée.
_PERIMETRE = CLIENT_CLAUSES[0] if (CLIENT_CLAUSES and len(CLIENT_CLAUSES) == 1) else "MULTI"

# Chemin DBFS par défaut des exports fichiers.
DEFAULT_BASE_PATH = (
    "dbfs:/FileStore/shared_uploads/cheickseko.bakayoko@axa.fr/itip_fiab_exports"
)


# ============================================================================
# TAG CLAUSE + COLLECTE DES ANALYSES
# ============================================================================

def tag_clause(df: DataFrame) -> DataFrame:
    """Place les colonnes d'identité CLAUSE / TYPE_CLAUSE en tête de table.

    En multi-périmètre la clause est portée par les données (dérivée via
    derive_clause_column avant agrégation) : chaque analyse est déjà ventilée
    par (CLAUSE, TYPE_CLAUSE). On ne fait que réordonner pour la lisibilité ;
    une table sans dimension clause est renvoyée telle quelle.
    """
    front = [c for c in ("CLAUSE", "TYPE_CLAUSE") if c in df.columns]
    if not front:
        return df
    rest = [c for c in df.columns if c not in front]
    return df.select(*front, *rest)


def collect_analyses(df_result: DataFrame) -> Dict[str, DataFrame]:
    """
    Assemble toutes les analyses restituables en un dict {nom: DataFrame},
    chaque table ventilée par (CLAUSE, TYPE_CLAUSE).

    derive_clause_column est appliqué une fois en amont pour matérialiser les
    colonnes CLAUSE / TYPE_CLAUSE attendues par chaque analyse.

    Note : diagnose_mrm_fanout n'est pas inclus (il imprime un récap et requiert
    le MRM clean en entrée) — il reste appelé à part dans main.
    """
    df_result = derive_clause_column(df_result)
    tables = {
        "suivi_consignes"      : analyze_suivi_consignes_global(df_result),
        "taux_chute"           : analyze_taux_chute(df_result),
        "consignes_pm"         : analyze_consignes_pm(df_result),
        "delete_non_suivies"   : analyze_delete_non_suivies(df_result),
        "provisionnement"      : study_provisionnement(df_result),
        "ventilation_cpt_only" : ventilate_cpt_only(df_result),
        "obs_tardives"         : analyze_obs_tardives(df_result),
    }
    return {name: tag_clause(df) for name, df in tables.items()}


def restituer_analyses(df_result: DataFrame, n: int = 100) -> Dict[str, DataFrame]:
    """Restitution console : .show() de chaque analyse (clause mentionnée).

    Renvoie le dict des tables (réutilisable pour un export ensuite).
    """
    tables = collect_analyses(df_result)
    for name, df in tables.items():
        print(f"\n===== {name}  (périmètre {CLIENT_NAME} / clauses {_PERIMETRE}) =====")
        df.show(n, truncate=False)
    return tables


# ============================================================================
# EXPORT MULTI-FORMAT (CSV / Parquet / Excel / Delta)
# ============================================================================

def _to_local(path: str) -> str:
    """Convertit un chemin dbfs:/... en /dbfs/... pour les writers locaux (pandas)."""
    return path.replace("dbfs:/", "/dbfs/", 1) if path.startswith("dbfs:/") else path


def _clause_dir(base_path: str) -> str:
    """Sous-dossier d'export propre au client/clause."""
    return f"{base_path.rstrip('/')}/{CLIENT_NAME}_{_PERIMETRE}"


def export_csv(tables: Dict[str, DataFrame], base_path: str, delimiter: str = ";") -> None:
    """Un CSV par analyse (coalesce(1), header), clause dans le nom de dossier."""
    out = _clause_dir(base_path)
    for name, df in tables.items():
        path = f"{out}/{name}_{_PERIMETRE}"
        (df.coalesce(1).write.mode("overwrite")
           .option("header", "true").option("delimiter", delimiter)
           .option("encoding", "UTF-8").csv(path))
        print(f"  ✓ [CSV]     {path}")


def export_parquet(tables: Dict[str, DataFrame], base_path: str) -> None:
    """Un Parquet par analyse, clause dans le nom de dossier."""
    out = _clause_dir(base_path)
    for name, df in tables.items():
        path = f"{out}/{name}_{_PERIMETRE}.parquet"
        df.coalesce(1).write.mode("overwrite").parquet(path)
        print(f"  ✓ [PARQUET] {path}")


def export_excel(tables: Dict[str, DataFrame], base_path: str) -> str:
    """Un seul .xlsx multi-onglets (un onglet par analyse, < 100k lignes)."""
    out_dir = _to_local(_clause_dir(base_path))
    os.makedirs(out_dir, exist_ok=True)
    path = f"{out_dir}/analyses_{CLIENT_NAME}_{_PERIMETRE}.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in tables.items():
            nb = df.count()
            if nb > 100_000:
                logger.warning("Onglet '%s' ignoré : %d lignes > 100 000.", name, nb)
                continue
            df.toPandas().to_excel(writer, sheet_name=name[:31], index=False)
            print(f"  ✓ [EXCEL]   onglet '{name[:31]}' ({nb:,} lignes)")
    print(f"  → fichier : {path}")
    return path


def export_delta(tables: Dict[str, DataFrame], schema: str) -> None:
    """Une table Delta metastore par analyse : <schema>.itip_<nom>_<clause>."""
    for name, df in tables.items():
        table = f"{schema}.itip_{name}_{_PERIMETRE}"
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true").saveAsTable(table))
        print(f"  ✓ [DELTA]   {table}")


def export_analyses(
    df_result   : DataFrame,
    base_path   : str = DEFAULT_BASE_PATH,
    formats     : Iterable[str] = ("csv", "parquet", "excel"),
    delta_schema: Optional[str] = None,
    delimiter   : str = ";",
) -> Dict[str, DataFrame]:
    """
    Exporte toutes les analyses dans les formats demandés (clause mentionnée
    en colonne ET dans le nom de fichier/table).

    Args:
        df_result    : résultat réconcilié (persisté + enrichi).
        base_path    : racine DBFS des exports fichiers (CSV/Parquet/Excel).
        formats      : sous-ensemble de {"csv", "parquet", "excel", "delta"}.
        delta_schema : schéma metastore cible (requis si "delta" ∈ formats).
        delimiter    : séparateur CSV.

    Returns:
        Le dict des tables exportées (taguées clause).
    """
    tables = collect_analyses(df_result)
    formats = {f.lower() for f in formats}
    print(f"\n[EXPORT] périmètre {CLIENT_NAME} / clauses {_PERIMETRE} → formats {sorted(formats)}")

    if "csv" in formats:
        export_csv(tables, base_path, delimiter)
    if "parquet" in formats:
        export_parquet(tables, base_path)
    if "excel" in formats:
        export_excel(tables, base_path)
    if "delta" in formats:
        if not delta_schema:
            logger.warning("Format 'delta' demandé sans delta_schema — ignoré.")
        else:
            export_delta(tables, delta_schema)

    print("[EXPORT] terminé\n")
    return tables
