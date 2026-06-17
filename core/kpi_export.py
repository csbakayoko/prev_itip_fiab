"""
Synthèse client — vue compacte type "schéma SODEXO".

Produit à chaque run :
  1. Une vue d'ensemble en 3 bulles (MRM / RETROUVÉS / COMPTE) avec, pour chaque
     sous-catégorie, la volumétrie en nombre de dossiers et en PM (€).
     RETROUVÉS = tous les matchés + tous les N+1 ; la base du taux de chute
     (hors « à supprimer » au compte) est détaillée dans les indicateurs.
  2. Un bloc d'INDICATEURS (taux de couverture, récupération tardive, taux de
     chute, niveaux de PM).
  3. Un bloc SUIVI DES CONSIGNES (taux de conformité par consigne, avec la
     volumétrie des dossiers à PM non nulle).

Décodage des grandeurs (depuis df_result + TYPE_RECONCILIATION) :

    MRM      = MATCHÉS + à supprimer + non mappés (total dossiers MRM en entrée)
    MATCHÉS  = clé principale (EXACT + WINDOW) + clé affinée (TRONC + TRONC_WINDOW)
               + récupération (IP / rechute / rechute tronquée)
    COMPTE   = MATCHÉS + récupérés N+1 (CPT_LATE) + obs tardives IT (anomalie,
               CPT_OBS_TARDIVE) + CPT_ONLY définitifs

    Univers CHUTE (stats globales) = matchés de l'inventaire courant, hors
    consigne « à supprimer » et hors statut inventaire NON. Les récupérés N+1
    (CPT_LATE) sont une ANALYSE SÉPARÉE, HORS stats globales : leur propre
    taux de chute + leur propre suivi de consignes (la consigne d'un N+1
    vient de l'inventaire N+1, pas de la revue auditée). Les « à supprimer »
    retrouvées et les repêchés statut NON sont analysés à part.
    Les obs tardives IT n'ont jamais matché → EXCLUES des métriques et des taux.

    PM : côté MRM (MRM_PM) pour les ventilations MRM, côté CPT (CPT_PM) pour les CPT.
"""

import logging

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from config import DATE_INVENTAIRE
from core.matching import categorize_mrm_conclusion
from core.synthese_contract import SyntheseScalars
from core.synthese_scalars import _scalars_from_rows
from core.synthese_render import render_synthese
from core._timing import timed_fn

logger = logging.getLogger(__name__)


# ============================================================================
# CALCUL DES SCALAIRES (une seule passe Spark)
# ============================================================================

def compute_synthese(df_result: DataFrame) -> SyntheseScalars:
    """
    Synthèse complète de df_result, en deux temps (découpage de testabilité) :

        1. `_collect_rows`       — LA passe Spark : agrégation par catégorie,
                                   collectée au driver (petit cardinal).
        2. `_scalars_from_rows`  — dérivation PURE-PYTHON des 75 scalaires depuis
                                   ces lignes (testable hors cluster).

    Colonnes attendues : TYPE_RECONCILIATION, MRM_PM, CPT_PM, MRM_CONCLUSION.
    """
    rows = _collect_rows(df_result)
    return _scalars_from_rows(rows, _resolve_date_inventaire(df_result))


def _collect_rows(df_result: DataFrame) -> list:
    """LA passe Spark : agrège df_result par (type × action × source × statut NON)
    en nb + PM MRM + PM CPT + volumétrie PM≠0, et collecte les lignes (petit
    cardinal) au driver. Tout le reste de la synthèse en dérive SANS Spark."""
    df = df_result.withColumn(
        "MRM_ACTION", categorize_mrm_conclusion(F.col("MRM_CONCLUSION"))
    )
    # LATE_SOURCE absent si aucune récupération tardive n'a tourné → colonne neutre.
    if "LATE_SOURCE" not in df.columns:
        df = df.withColumn("LATE_SOURCE", F.lit(None).cast("string"))
    # Statut inventaire NON : exclu de l'univers de chute. Structurellement les
    # matchés viennent du MRM OUI (split en amont) — la dimension rend la règle
    # explicite et robuste si un MRM non scindé est passé en entrée.
    df = df.withColumn(
        "IS_STATUT_NON",
        F.coalesce(F.upper(F.trim(F.col("MRM_STATUT_INV"))) == "NON", F.lit(False))
        if "MRM_STATUT_INV" in df.columns else F.lit(False),
    )
    return (
        df.groupBy("TYPE_RECONCILIATION", "MRM_ACTION", "LATE_SOURCE", "IS_STATUT_NON")
        .agg(
            F.count("*").alias("nb"),
            F.coalesce(F.sum("MRM_PM"), F.lit(0.0)).alias("pm_mrm"),
            F.coalesce(F.sum("CPT_PM"), F.lit(0.0)).alias("pm_cpt"),
            # Volumétrie des dossiers dont la PM est non nulle (non-null ET ≠ 0)
            F.sum(F.when(F.col("MRM_PM").isNotNull() & (F.col("MRM_PM") != 0), 1).otherwise(0)).alias("nb_pm_mrm_nz"),
            F.sum(F.when(F.col("CPT_PM").isNotNull() & (F.col("CPT_PM") != 0), 1).otherwise(0)).alias("nb_pm_cpt_nz"),
        )
        .collect()
    )


def kas_totaux(d: dict) -> dict:
    """Totaux KEEP+ADD+STUDY depuis les scalaires de compute_synthese.

    Sert à la conformité globale (nb, conformes). Les champs PM sont les
    Σ des consignes KAS : pour la chute, utiliser d["metrics_pm_*"]
    (univers = matchés inventaire courant hors « à supprimer » / statut NON,
    qui inclut aussi les sans consigne reconnue — d["hors_consigne_*"])."""
    kas = [d["consignes"][k] for k in ("À conserver", "À étudier", "À ajouter")]
    return {
        "nb"     : sum(c["nb"]     for c in kas),
        "conf"   : sum(c["conf"]   for c in kas),
        "pm_mrm" : sum(c["pm_mrm"] for c in kas),
        "pm_cpt" : sum(c["pm_cpt"] for c in kas),
        "delta"  : sum(c["delta"]  for c in kas),
    }


def _resolve_date_inventaire(df_result: DataFrame) -> str:
    """Date d'inventaire : figée (profile) ou max(MRM_D_INVENTAIRE) si 'auto'."""
    if DATE_INVENTAIRE != "auto":
        return DATE_INVENTAIRE
    if "MRM_D_INVENTAIRE" not in df_result.columns:
        return "n/d"
    row = df_result.agg(
        F.date_format(F.max("MRM_D_INVENTAIRE"), "dd/MM/yyyy").alias("d")
    ).first()
    return (row and row["d"]) or "n/d"


@timed_fn("print_synthese")
def print_synthese(df_result: DataFrame) -> dict:
    """Calcule + affiche la synthèse. Retourne les scalaires."""
    d = compute_synthese(df_result)
    print("\n" + render_synthese(d) + "\n")
    return d
