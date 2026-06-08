"""Enrichissement du résultat : tag obs tardives IT + tags orphelins + rejet final."""

import logging

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import Dict, List, Optional, Tuple

from config import (
    LATE_IT_GARANTIE, OBS_TARDIVE_LABEL, ORPHAN_PM_THRESHOLD, ORPHAN_FIN_ANNEE_MOIS,
)
from modules.matching import categorize_mrm_conclusion
from modules.analysis.helpers import _inventory_year

logger = logging.getLogger(__name__)


def flag_late_it_observations(
    df_result   : DataFrame,
    garantie_col: str = "CPT_GARANTIE",
    date_col    : str = "CPT_D_SURVENANCE",
) -> DataFrame:
    """
    Tague en CPT_OBS_TARDIVE les CPT_ONLY qui sont des observations tardives d'IT.

    Un CPT_ONLY resté orphelin après la récupération N+1 (donc absent du MRM
    courant ET du N+1 → « pas dans deux exercices successifs ») est calé en
    observation tardive lorsque :
        - garantie == LATE_IT_GARANTIE (incapacité de travail),
        - survenance en fin d'année (mois ∈ ORPHAN_FIN_ANNEE_MOIS),
        - année de survenance == année d'inventaire (si dérivable de DATE_INVENTAIRE).

    Hypothèse métier : la couverture IT a vraisemblablement pris fin avant la date
    d'inventaire de l'exercice suivant (inventaire MRM réalisé 2× par an), d'où
    l'absence de contrepartie MRM. Ces lignes n'ont donc pas de colonnes MRM_*.

    IMPORTANT — ce ne sont PAS des dossiers retrouvés : ils n'ont jamais matché.
    On les tague comme anomalie (déclaration probable de fin d'année) mais on les
    EXCLUT des taux (matching / récupération) et des calculs PM / taux de chute.
    Le label distinct OBS_TARDIVE_LABEL (≠ CPT_LATE) garantit cette exclusion par
    construction : tout code basé sur MATCH_LABELS / CPT_LATE les ignore.

    Lignes taguées :
        TYPE_RECONCILIATION → OBS_TARDIVE_LABEL ("CPT_OBS_TARDIVE")
        LATE_SOURCE         → "OBS_TARDIVE_IT"  (traçabilité de l'origine)

    À appeler APRÈS recover_late_declarations et AVANT enrich_result_tags (pour
    que ces dossiers ne soient plus comptés/tagués comme CPT_ONLY).
    """
    inv_year    = _inventory_year()
    is_cpt_only = F.col("TYPE_RECONCILIATION") == "CPT_ONLY"
    eligible = (
        is_cpt_only
        & (F.col(garantie_col).cast("int") == F.lit(LATE_IT_GARANTIE))
        & F.month(F.col(date_col)).isin(*ORPHAN_FIN_ANNEE_MOIS)
        & (F.year(F.col(date_col)) == F.lit(inv_year) if inv_year is not None else F.lit(True))
    )

    df = df_result
    if "LATE_SOURCE" not in df.columns:
        df = df.withColumn("LATE_SOURCE", F.lit(None).cast("string"))

    return (
        df.withColumn(
            "TYPE_RECONCILIATION",
            F.when(eligible, F.lit(OBS_TARDIVE_LABEL)).otherwise(F.col("TYPE_RECONCILIATION")),
        )
        .withColumn(
            "LATE_SOURCE",
            F.when(eligible, F.lit("OBS_TARDIVE_IT")).otherwise(F.col("LATE_SOURCE")),
        )
    )


def enrich_result_tags(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    date_col      : str = "CPT_D_SURVENANCE",
    pm_col        : str = "CPT_PM",
) -> DataFrame:
    """
    Ajoute deux colonnes persistantes au résultat de réconciliation :

    1. MRM_ACTION  : consigne MRM reformatée (MRM_KEEP / MRM_ADD / MRM_STUDY /
                     MRM_DELETE), null si pas de conclusion. Conservée pour le
                     reporting et Power BI (évite de recalculer ailleurs).

    2. TAG_CPT_ONLY : segmentation actionnable des CPT_ONLY définitifs —
                      DECLA_TARDIVE_PROBABLE  : survenance en fin d'année
                          d'inventaire (mois ∈ ORPHAN_FIN_ANNEE_MOIS) → sinistre
                          probablement déclaré après la clôture MRM.
                      ORPHELIN_MONTANT_ELEVE  : PM CPT > ORPHAN_PM_THRESHOLD.
                      ORPHELIN_A_ANALYSER     : les autres orphelins.
                      null pour les lignes non CPT_ONLY.

    Args:
        df_result      : DataFrame réconcilié (sortie waterfall + recovery)
        conclusion_col : Colonne conclusion MRM brute
        date_col       : Colonne date de survenance CPT
        pm_col         : Colonne PM CPT

    Returns:
        df_result enrichi de MRM_ACTION et TAG_CPT_ONLY.
    """
    df = df_result.withColumn("MRM_ACTION", categorize_mrm_conclusion(F.col(conclusion_col)))

    is_cpt_only = F.col("TYPE_RECONCILIATION") == "CPT_ONLY"
    inv_year    = _inventory_year()
    fin_annee   = (
        is_cpt_only
        & F.month(F.col(date_col)).isin(*ORPHAN_FIN_ANNEE_MOIS)
        & (F.year(F.col(date_col)) == F.lit(inv_year) if inv_year is not None else F.lit(True))
    )

    return df.withColumn(
        "TAG_CPT_ONLY",
        F.when(fin_annee,                                       "DECLA_TARDIVE_PROBABLE")
         .when(is_cpt_only & (F.col(pm_col) > ORPHAN_PM_THRESHOLD), "ORPHELIN_MONTANT_ELEVE")
         .when(is_cpt_only,                                     "ORPHELIN_A_ANALYSER")
         .otherwise(None)
    )


def drop_unmatched_inventory_non(
    df_result : DataFrame,
    statut_col: str = "MRM_STATUT_INV",
) -> DataFrame:
    """
    Jette les dossiers MRM au statut inventaire NON restés NON matchés en fin de
    pipeline (TYPE_RECONCILIATION == "MRM_MISSING").

    Le statut NON est ouvert au chargement pour permettre le REPÊCHAGE : un MRM
    NON qui matche un compte est légitime (il est dans le compte) et conservé.
    Mais un MRM NON qui n'a jamais matché n'est pas remonté à la direction
    financière → il n'a aucune contrepartie comptable : on le jette purement et
    simplement (hors métriques ET hors export). Nettoyage tracé.

    À appeler en TOUTE FIN de pipeline (après matching + recovery + tags), juste
    avant la persistance et les analyses.
    """
    if statut_col not in df_result.columns:
        return df_result

    is_non_unmatched = (
        (F.col("TYPE_RECONCILIATION") == "MRM_MISSING")
        & (F.upper(F.trim(F.col(statut_col))) == F.lit("NON"))
    )
    n_drop = df_result.filter(is_non_unmatched).count()
    if n_drop:
        logger.info(
            "Rejet MRM statut NON non matchés (MRM_MISSING) : %d dossier(s) jeté(s) "
            "— hors métriques et hors export.", n_drop,
        )
    return df_result.filter(~is_non_unmatched)

