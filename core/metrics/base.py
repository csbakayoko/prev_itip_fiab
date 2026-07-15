"""
Socle partagé de la couche métriques — chemins d'export et helpers Spark.

Consommé par les modules frères (scalaires, agregats, coherence, export) et
par viz : libellés d'exercice (EXERCICE_*), blocs d'ancienneté (BLOC_*),
univers du taux de chute, chemins DBFS. Les dimensions CLAUSE / TYPE_COMPTE /
REMONTE_DF sont persistées par le PIPELINE (core.match.recovery.
derive_clause_column via enrich_result_tags) — re-exportée ici pour l'usage
ad hoc (elle est idempotente : passthrough si les colonnes existent déjà).
"""

from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from config import (
    CLIENT_NAME, MATCH_LABELS,
    EXPORT_BASE_PATH, PERIMETRE_LABEL,
)
from core.match.matching import categorize_mrm_conclusion, derive_clause_column  # noqa: F401 — ré-export
from core.synthese.synthese_contract import SyntheseScalars


# ============================================================================
# CHEMINS D'EXPORT (DBFS)
# ============================================================================

# Libellé de périmètre (config.PERIMETRE_LABEL) : nomme les sorties fichiers
# et alimente la colonne PERIMETRE des tables. La clause réelle reste DANS les tables.
_PERIMETRE = PERIMETRE_LABEL


def _to_local(path: str) -> str:
    """Convertit un chemin dbfs:/... en /dbfs/... pour les writers locaux (pandas)."""
    return path.replace("dbfs:/", "/dbfs/", 1) if path.startswith("dbfs:/") else path


def output_dir(base_path: str = EXPORT_BASE_PATH, sub: str = "") -> str:
    """Sous-dossier d'export propre au périmètre (<base>/<CLIENT>_<PERIM>[/sub])."""
    out = f"{base_path.rstrip('/')}/{CLIENT_NAME}_{_PERIMETRE}"
    return f"{out}/{sub}" if sub else out


# ============================================================================
# HELPERS SPARK (univers de chute)
# ============================================================================

def _with_mrm_action(df: DataFrame) -> DataFrame:
    """MRM_ACTION persistée par enrich_result_tags ; recalculée si absente."""
    if "MRM_ACTION" in df.columns:
        return df
    return df.withColumn("MRM_ACTION", categorize_mrm_conclusion(F.col("MRM_CONCLUSION")))


def _filter_chute_universe(df: DataFrame) -> DataFrame:
    """Univers des taux de chute, les deux exercices réunis : matchés
    inventaire courant (stats globales) + récupérés N+1 (analyse séparée),
    hors consigne « à supprimer » et hors statut inventaire NON (même règle
    que kpi_export.compute_synthese) — la séparation se fait ensuite par la
    colonne EXERCICE. Les sans-consigne reconnue (MRM_ACTION null) restent
    inclus. CPT_OBS_TARDIVE / CPT_RECUP_NON exclus (jamais matchés / PM MRM
    = 0)."""
    cond = (
        F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS) + ["CPT_LATE"])
        # null-safe : une MRM_ACTION absente/inconnue reste dans l'univers.
        & F.coalesce(F.col("MRM_ACTION") != "MRM_DELETE", F.lit(True))
    )
    if "MRM_STATUT_INV" in df.columns:
        cond &= F.coalesce(F.upper(F.trim(F.col("MRM_STATUT_INV"))) != "NON", F.lit(True))
    return df.filter(cond)


def _mois_label_expr(date_col: str) -> F.Column:
    """Abréviation française du mois (Jan … Déc) depuis une colonne date."""
    labels = ["Jan", "Fév", "Mar", "Avr", "Mai", "Jun",
              "Jul", "Aoû", "Sep", "Oct", "Nov", "Déc"]
    m = F.month(F.col(date_col))
    expr = F.lit("Déc")
    for i, lbl in enumerate(labels[:-1], start=1):
        expr = F.when(m == i, lbl).otherwise(expr)
    return expr


# Libellés des deux EXERCICES de chute (chute_par_exercice, chute_par_type_compte) :
# inventaire courant = les stats globales ; N+1 = analyse séparée.
EXERCICE_INV    = "Inventaire courant"
EXERCICE_N1     = "Récupérés N+1"
_EXERCICE_ORDRE = {EXERCICE_INV: 0, EXERCICE_N1: 1}


# Blocs d'ancienneté = année de survenance relative à l'année d'inventaire.
# La méthode d'inventaire diffère selon l'année (revue tête par tête sur N-1) →
# le taux de chute est découpé N / N-1 / N-2 et antérieur (cf. chute_par_anciennete).
BLOC_N          = "N"
BLOC_N1         = "N-1"
BLOC_N2_PLUS    = "N-2 et antérieur"
BLOC_INDET      = "Indéterminée"        # année de survenance nulle / inventaire non daté
_BLOC_ORDRE     = {BLOC_N: 0, BLOC_N1: 1, BLOC_N2_PLUS: 2, BLOC_INDET: 3}


def _annee_inventaire(d: SyntheseScalars) -> Optional[int]:
    """Année d'inventaire depuis d["date_inventaire"] ('dd/MM/yyyy'), None sinon."""
    try:
        return int(str(d["date_inventaire"]).split("/")[-1])
    except (ValueError, KeyError, TypeError):
        return None


def _bloc_anciennete_expr(date_col: str, inv_year: Optional[int]) -> F.Column:
    """Bloc d'ancienneté (N / N-1 / N-2 et antérieur) depuis l'année de survenance.

    inv_year None (inventaire non daté) ou année de survenance nulle → BLOC_INDET.
    """
    if inv_year is None:
        return F.lit(BLOC_INDET)
    y = F.year(F.col(date_col))
    return (
        F.when(y == F.lit(inv_year),     F.lit(BLOC_N))
         .when(y == F.lit(inv_year - 1), F.lit(BLOC_N1))
         .when(y <= F.lit(inv_year - 2), F.lit(BLOC_N2_PLUS))
         .otherwise(F.lit(BLOC_INDET))
    )
