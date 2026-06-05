"""Provisionnement : sous/sur/conforme, écarts par tranches, étude globale."""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import Dict, List, Optional, Tuple

from config import MATCH_LABELS
from modules.matching import categorize_mrm_conclusion
from modules.analysis.helpers import (
    _with_mrm_action, _filter_matched_keep_add_study, DEFAULT_ECART_TRANCHES,
)


def analyze_provisionnement(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    clause_col    : str = "CLAUSE",
) -> DataFrame:
    """
    Niveau de provisionnement par rapport à la référence MRM, par clause.

    Catégories :
    ┌──────────────────┬──────────────────────────────────────┐
    │ Catégorie        │ Condition                            │
    ├──────────────────┼──────────────────────────────────────┤
    │ SOUS_PROVISIONNE │ PM_CPT < PM_MRM  (risque financier)  │
    │ CONFORME         │ PM_CPT = PM_MRM  (situation idéale)  │
    │ SUR_PROVISIONNE  │ PM_CPT > PM_MRM  (marge de sécurité) │
    └──────────────────┴──────────────────────────────────────┘

    Colonnes :
        CLAUSE, MRM_ACTION, CATEGORIE_PROVISION,
        nb_dossiers, pm_mrm, pm_cpt, ecart, pct_nb

    Args:
        df_result      : DataFrame résultat du waterfall enrichi avec CLAUSE
        conclusion_col : Colonne conclusion MRM brute
        clause_col     : Colonne clause

    Returns:
        DataFrame agrégé par (CLAUSE, MRM_ACTION, CATEGORIE_PROVISION)
    """
    # Fenêtre par (clause, type_clause, consigne) pour les pourcentages
    window_consigne = Window.partitionBy(clause_col, "TYPE_CLAUSE", "MRM_ACTION")

    return (
        _filter_matched_keep_add_study(_with_mrm_action(df_result, conclusion_col))
        .withColumn("ecart_pm_abs", F.abs(F.col("MRM_PM") - F.col("CPT_PM")))
        .withColumn("CATEGORIE_PROVISION",
            F.when(F.col("CPT_PM") < F.col("MRM_PM"), "SOUS_PROVISIONNE")
             .when(F.col("CPT_PM") > F.col("MRM_PM"), "SUR_PROVISIONNE")
             .otherwise("CONFORME")
        )
        .groupBy(clause_col, "TYPE_CLAUSE", "MRM_ACTION", "CATEGORIE_PROVISION")
        .agg(
            F.count("*").alias("nb_dossiers"),
            F.sum("MRM_PM").alias("pm_mrm"),
            F.sum("CPT_PM").alias("pm_cpt"),
            F.sum("ecart_pm_abs").alias("ecart"),
        )
        .withColumn("total_nb_consigne", F.sum("nb_dossiers").over(window_consigne))
        .withColumn("pct_nb", F.round(F.col("nb_dossiers") / F.col("total_nb_consigne") * 100, 2))
        .drop("total_nb_consigne")
    )


def analyze_ecarts_tranches(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    tranches      : Optional[List[Tuple[Optional[float], Optional[float], str]]] = None,
    clause_col    : str = "CLAUSE",
) -> DataFrame:
    """
    Distribution des dossiers SOUS / SUR-provisionnés par tranches d'écart PM.

    Utile pour bar charts détaillés : combien de dossiers ont un écart
    de 1K–5K, 5K–10K, etc. — par clause, pour KEEP ADD et STUDY séparément.

    Tranches par défaut (€) :
    ┌────────────────┬──────────────────────────────────────────────────┐
    │ Tranche        │ Condition sur |PM_MRM - PM_CPT|                  │
    ├────────────────┼──────────────────────────────────────────────────┤
    │ < 1K           │ ecart_abs < 1 000                                │
    │ 1K – 5K        │ 1 000  ≤ ecart_abs < 5 000                      │
    │ 5K – 10K       │ 5 000  ≤ ecart_abs < 10 000                     │
    │ 10K – 50K      │ 10 000 ≤ ecart_abs < 50 000                     │
    │ 50K – 100K     │ 50 000 ≤ ecart_abs < 100 000                    │
    │ > 100K         │ ecart_abs ≥ 100 000                              │
    └────────────────┴──────────────────────────────────────────────────┘

    Colonnes :
        CLAUSE, MRM_ACTION, CATEGORIE_PROVISION, TRANCHE_ECART,
        nb_dossiers, pm_mrm, pm_cpt, ecart_total, ecart_moyen, ecart_max,
        pct_nb_dans_categorie, pct_pm_dans_categorie

    Args:
        df_result      : DataFrame résultat du waterfall enrichi avec CLAUSE
        conclusion_col : Colonne conclusion MRM brute
        tranches       : Tranches custom — liste de (min, max, label)
                         min=None → pas de borne basse, max=None → pas de borne haute
        clause_col     : Colonne clause

    Returns:
        DataFrame agrégé par (CLAUSE, MRM_ACTION, CATEGORIE_PROVISION, TRANCHE_ECART)
    """
    active_tranches = tranches or DEFAULT_ECART_TRANCHES

    # ── Préparation des dossiers à analyser ─────────────────────────
    df_base = (
        _filter_matched_keep_add_study(_with_mrm_action(df_result, conclusion_col))
        .withColumn("ecart_pm_abs",  F.abs(F.col("MRM_PM") - F.col("CPT_PM")))
        .withColumn("ecart_pm_signe", F.col("MRM_PM") - F.col("CPT_PM"))
        .withColumn("CATEGORIE_PROVISION",
            F.when(F.col("CPT_PM") < F.col("MRM_PM"), "SOUS_PROVISIONNE")
             .when(F.col("CPT_PM") > F.col("MRM_PM"), "SUR_PROVISIONNE")
             .otherwise("CONFORME")
        )
        # Conforme = écart nul → pas pertinent pour analyse par tranches
        .filter(F.col("CATEGORIE_PROVISION") != "CONFORME")
    )

    # ── Affectation de la tranche ────────────────────────────────────
    tranche_expr = F.lit("AUTRE")
    for (low, high, label) in reversed(active_tranches):
        if low is None and high is not None:
            cond = F.col("ecart_pm_abs") < high
        elif low is not None and high is None:
            cond = F.col("ecart_pm_abs") >= low
        else:
            cond = (F.col("ecart_pm_abs") >= low) & (F.col("ecart_pm_abs") < high)
        tranche_expr = F.when(cond, F.lit(label)).otherwise(tranche_expr)

    df_with_tranche = df_base.withColumn("TRANCHE_ECART", tranche_expr)

    # ── Ordre des tranches pour le tri (position dans la liste) ──────
    tranche_order = {label: i for i, (_, _, label) in enumerate(active_tranches)}
    tranche_order_expr = F.create_map(
        *[x for label, idx in tranche_order.items()
          for x in (F.lit(label), F.lit(idx))]
    )

    # ── Fenêtre pour pourcentages dans chaque (clause, type_clause, consigne, catégorie) ──
    window_cat = Window.partitionBy(clause_col, "TYPE_CLAUSE", "MRM_ACTION", "CATEGORIE_PROVISION")

    # ── Agrégation par (CLAUSE, TYPE_CLAUSE, MRM_ACTION, CATEGORIE_PROVISION, TRANCHE_ECART)
    return (
        df_with_tranche
        .groupBy(clause_col, "TYPE_CLAUSE", "MRM_ACTION", "CATEGORIE_PROVISION", "TRANCHE_ECART")
        .agg(
            F.count("*").alias("nb_dossiers"),
            F.sum("MRM_PM").alias("pm_mrm"),
            F.sum("CPT_PM").alias("pm_cpt"),
            F.sum("ecart_pm_abs").alias("ecart_total"),
            F.avg("ecart_pm_abs").alias("ecart_moyen"),
            F.max("ecart_pm_abs").alias("ecart_max"),
        )
        .withColumn("total_nb_cat",  F.sum("nb_dossiers").over(window_cat))
        .withColumn("total_pm_cat",  F.sum("ecart_total").over(window_cat))
        .withColumn("pct_nb_dans_categorie",
            F.round(F.col("nb_dossiers") / F.col("total_nb_cat") * 100, 2))
        .withColumn("pct_pm_dans_categorie",
            F.round(F.col("ecart_total") / F.col("total_pm_cat") * 100, 2))
        .withColumn("ecart_moyen", F.round(F.col("ecart_moyen"), 2))
        .withColumn("ecart_max",   F.round(F.col("ecart_max"),   2))
        # Tri logique par tranche
        .withColumn("_ordre_tranche", tranche_order_expr[F.col("TRANCHE_ECART")])
        .orderBy(clause_col, "MRM_ACTION", "CATEGORIE_PROVISION", "_ordre_tranche")
        .drop("total_nb_cat", "total_pm_cat", "_ordre_tranche")
    )


def study_provisionnement(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
) -> DataFrame:
    """
    Étude du provisionnement sur les dossiers MATCHÉS (hors consigne à supprimer).

    Compare la provision comptable (CPT_PM) à la référence MRM (MRM_PM) :
        SOUS_PROVISIONNE : CPT_PM < MRM_PM  (risque — provision insuffisante)
        SUR_PROVISIONNE  : CPT_PM > MRM_PM  (marge de sécurité)
        CONFORME         : CPT_PM = MRM_PM

    NB : convention intuitive (sous-prov = on a provisionné MOINS que le besoin).
    Écart = MRM_PM − CPT_PM (positif = sous-provisionnement). Taux de chute =
    Σécart / ΣMRM_PM × 100, cohérent avec la synthèse.

    Univers : matchs légitimes (MATCH_LABELS) + déclarations tardives (CPT_LATE),
    consigne MRM_DELETE exclue. PM nulles ramenées à 0 pour la comparaison.

    Colonnes (une ligne par catégorie, + tri par nb décroissant) :
        CATEGORIE_PROVISION, NB_DOSSIERS, PCT_NB,
        PM_MRM, PM_CPT, PCT_PM_MRM,
        ECART_SIGNE, ECART_ABS_TOTAL, ECART_ABS_MOYEN, ECART_ABS_MAX,
        TAUX_CHUTE

    Returns:
        DataFrame de stats par catégorie, prêt pour visualisation.
    """
    matched = list(MATCH_LABELS) + ["CPT_LATE"]
    pm_mrm  = F.coalesce(F.col("MRM_PM"), F.lit(0.0))
    pm_cpt  = F.coalesce(F.col("CPT_PM"), F.lit(0.0))

    df = (
        df_result
        .filter(F.col("TYPE_RECONCILIATION").isin(matched))
        .withColumn("_ACTION", categorize_mrm_conclusion(F.col(conclusion_col)))
        .filter(F.col("_ACTION").isNull() | (F.col("_ACTION") != "MRM_DELETE"))
        .withColumn("ECART", pm_mrm - pm_cpt)            # > 0 → sous-provisionné
        .withColumn("ECART_ABS", F.abs(pm_mrm - pm_cpt))
        .withColumn("CATEGORIE_PROVISION",
            F.when(pm_cpt < pm_mrm, "SOUS_PROVISIONNE")
             .when(pm_cpt > pm_mrm, "SUR_PROVISIONNE")
             .otherwise("CONFORME"))
    )

    w = Window.partitionBy()   # totaux globaux pour les pourcentages
    return (
        df.groupBy("CATEGORIE_PROVISION")
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.round(F.sum("MRM_PM"), 2).alias("PM_MRM"),
            F.round(F.sum("CPT_PM"), 2).alias("PM_CPT"),
            F.round(F.sum("ECART"), 2).alias("ECART_SIGNE"),
            F.round(F.sum("ECART_ABS"), 2).alias("ECART_ABS_TOTAL"),
            F.round(F.avg("ECART_ABS"), 2).alias("ECART_ABS_MOYEN"),
            F.round(F.max("ECART_ABS"), 2).alias("ECART_ABS_MAX"),
        )
        .withColumn("PCT_NB",
            F.round(F.col("NB_DOSSIERS") / F.sum("NB_DOSSIERS").over(w) * 100, 1))
        .withColumn("PCT_PM_MRM",
            F.round(F.col("PM_MRM") / F.sum("PM_MRM").over(w) * 100, 1))
        .withColumn("TAUX_CHUTE",
            F.round(F.when(F.col("PM_MRM") != 0,
                           F.col("ECART_SIGNE") / F.col("PM_MRM") * 100).otherwise(0.0), 2))
        .orderBy(F.desc("NB_DOSSIERS"))
    )

