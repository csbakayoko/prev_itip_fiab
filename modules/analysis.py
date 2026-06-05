"""
Analyse métier de la réconciliation CPT/MRM.

Sections :
    1. Consignes MRM      — audit conformité (KEEP / STUDY / ADD / DELETE)
    2. Taux de chute      — écarts PM entre CPT et MRM
    3. Provisionnement    — sous / conforme / sur-provisionnement
    4. Écarts détaillés   — distribution des écarts par tranches (→ bar chart viz)
    5. Orphelins          — analyse par mois + tranche PM (Power BI) + détail brut (export CSV)

Contrat de colonnes attendues dans df_result (sorti du waterfall + derive_clause_column) :
    CLAUSE               : clause ou "GLOBAL" (ajouté par derive_clause_column)
    TYPE_RECONCILIATION  : un des labels de MATCH_LABELS | "CPT_ONLY" | "MRM_MISSING" | "MRM_DELETE"
    MRM_CONCLUSION       : conclusion MRM brute (texte libre, préfixe MRM_ après clean_mrm)
    CPT_PM / MRM_PM      : montants PM préfixés
    CPT_PSAP / MRM_PSAP  : montants PSAP préfixés

Toutes les fonctions d'analyse acceptent un paramètre `clause_col` (défaut: "CLAUSE")
ajouté en premier dans chaque groupBy() et Window.partitionBy() — ce qui permet à
Power BI de filtrer les visuels via une liste déroulante sur la clause.
"""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import Dict, List, Optional, Tuple

from config import (
    MATCH_LABELS,
    DATE_INVENTAIRE,
    LATE_IT_GARANTIE,
    OBS_TARDIVE_LABEL,
    ORPHAN_PM_THRESHOLD,
    ORPHAN_FIN_ANNEE_MOIS,
)
from modules.matching import categorize_mrm_conclusion


# ============================================================================
# HELPERS INTERNES
# ============================================================================

def _with_mrm_action(df: DataFrame, conclusion_col: str) -> DataFrame:
    """Ajoute la colonne MRM_ACTION via categorize_mrm_conclusion."""
    return df.withColumn(
        "MRM_ACTION",
        categorize_mrm_conclusion(F.col(conclusion_col))
    )


def _statut_inv_dim(df: DataFrame, col: str = "MRM_STATUT_INV") -> List[str]:
    """
    Renvoie [col] si la colonne statut inventaire est présente, sinon [].

    Permet d'ajouter MRM_STATUT_INV comme dimension de ventilation (OUI/NON) dans
    les agrégats exportés sans casser quand la source ne fournit pas la colonne.
    Un MRM avec statut NON n'est pas remonté à la direction financière : la
    dissociation OUI/NON est portée par cette dimension côté Power BI.
    """
    return [col] if col in df.columns else []


def _filter_matched_keep_add_study(df: DataFrame) -> DataFrame:
    """Filtre les dossiers matchés avec consigne KEEP, STUDY ou ADD."""
    return df.filter(
        F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS)) &
        F.col("MRM_ACTION").isin("MRM_KEEP", "MRM_ADD", "MRM_STUDY")
    )


# ============================================================================
# SECTION 1 : SUIVI DES CONSIGNES MRM
# ============================================================================

def analyze_suivi_consignes(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    clause_col    : str = "CLAUSE",
) -> Tuple[DataFrame, DataFrame]:
    """
    Audit de conformité des consignes MRM par clause.

    Logique métier :
    ┌────────────┬──────────────────────┬───────────────────────┐
    │ Consigne   │ TYPE_RECONCILIATION  │ Résultat audit        │
    ├────────────┼──────────────────────┼───────────────────────┤
    │ MRM_KEEP   │ MATCH_*              │ CONFORME              │
    │ MRM_KEEP   │ MRM_MISSING          │ NON_CONFORME          │
    │ MRM_ADD    │ MATCH_*              │ CONFORME              │
    │ MRM_ADD    │ MRM_MISSING          │ NON_CONFORME          │
    │ MRM_STUDY  │ MATCH_*              │ CONFORME              │
    │ MRM_STUDY  │ MRM_MISSING          │ NON_CONFORME          │
    │ MRM_DELETE │ MRM_MISSING          │ CONFORME              │
    │ MRM_DELETE │ MATCH_*              │ NON_CONFORME          │
    └────────────┴──────────────────────┴───────────────────────┘

    Colonnes du résultat summary :
        CLAUSE, MRM_ACTION, RESULTAT_AUDIT, nb_dossiers, pm_mrm, pct_nb, pct_pm

    Args:
        df_result      : DataFrame résultat du waterfall enrichi avec CLAUSE
        conclusion_col : Colonne conclusion MRM brute
        clause_col     : Colonne clause (ajoutée par derive_clause_column)

    Returns:
        Tuple (df_audit_summary, df_audit_detail)
    """
    is_matched = F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS))

    df_audit = (
        _with_mrm_action(df_result, conclusion_col)
        .filter(F.col("MRM_ACTION").isNotNull())
        .withColumn(
            "RESULTAT_AUDIT",
            F.when(is_matched  & (F.col("MRM_ACTION") == "MRM_KEEP"),   "CONFORME")
             .when(~is_matched & (F.col("MRM_ACTION") == "MRM_KEEP"),   "NON_CONFORME")
             .when(~is_matched & (F.col("MRM_ACTION") == "MRM_DELETE"), "CONFORME")
             .when(is_matched  & (F.col("MRM_ACTION") == "MRM_DELETE"), "NON_CONFORME")
             .when(is_matched  & (F.col("MRM_ACTION") == "MRM_STUDY"),  "CONFORME")
             .when(~is_matched & (F.col("MRM_ACTION") == "MRM_STUDY"),  "NON_CONFORME")
             .when(is_matched  & (F.col("MRM_ACTION") == "MRM_ADD"),    "CONFORME")
             .when(~is_matched & (F.col("MRM_ACTION") == "MRM_ADD"),    "NON_CONFORME")
             .otherwise("AUTRE")
        )
    )

    # Dimension statut inventaire (OUI/NON) si disponible — ventilation exportable.
    statut_dim = _statut_inv_dim(df_audit)

    # Fenêtre partitionnée par (clause, type_clause, [statut], consigne) pour les pourcentages
    window_consigne = Window.partitionBy(clause_col, "TYPE_CLAUSE", *statut_dim, "MRM_ACTION")

    df_audit_summary = (
        df_audit
        .groupBy(clause_col, "TYPE_CLAUSE", *statut_dim, "MRM_ACTION", "RESULTAT_AUDIT")
        .agg(
            F.count("*").alias("nb_dossiers"),
            F.sum("MRM_PM").alias("pm_mrm"),
        )
        .withColumn("total_consigne_nb", F.sum("nb_dossiers").over(window_consigne))
        .withColumn("total_consigne_pm", F.sum("pm_mrm").over(window_consigne))
        .withColumn("pct_nb", F.round(F.col("nb_dossiers") / F.col("total_consigne_nb") * 100, 2))
        .withColumn("pct_pm", F.round(F.col("pm_mrm")      / F.col("total_consigne_pm") * 100, 2))
        .drop("total_consigne_nb", "total_consigne_pm")
    )

    return df_audit_summary, df_audit


def analyze_consignes_ratios(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    clause_col    : str = "CLAUSE",
) -> DataFrame:
    """
    Ratios simplifiés par (CLAUSE × MRM_ACTION × TYPE_RECONCILIATION).
    Version allégée pour les visuels synthétiques Power BI.

    Les ratios sont calculés dans le scope de chaque clause via une fenêtre
    — aucun .collect() supplémentaire.

    Colonnes : CLAUSE, MRM_ACTION, TYPE_RECONCILIATION, nb_dossiers, pm_mrm,
               ratio_nombre_pct, ratio_pm_pct

    Args:
        df_result      : DataFrame résultat du waterfall enrichi avec CLAUSE
        conclusion_col : Colonne conclusion MRM brute
        clause_col     : Colonne clause

    Returns:
        DataFrame agrégé par (CLAUSE, MRM_ACTION, TYPE_RECONCILIATION)
    """
    df_mrm = (
        _with_mrm_action(df_result, conclusion_col)
        .filter(F.col("MRM_ACTION").isNotNull())
        # MRM_DELETE exclus : ces dossiers ne font pas partie du périmètre de
        # matching — les inclure dans le total fausserait les ratios KEEP/STUDY/ADD.
        .filter(F.col("MRM_ACTION") != "MRM_DELETE")
    )

    # Fenêtre par (clause, type_clause) pour calculer les totaux dans le scope de chaque clause
    window_clause = Window.partitionBy(clause_col, "TYPE_CLAUSE")

    return (
        df_mrm
        .groupBy(clause_col, "TYPE_CLAUSE", "MRM_ACTION", "TYPE_RECONCILIATION")
        .agg(
            F.count("*").alias("nb_dossiers"),
            F.sum("MRM_PM").alias("pm_mrm"),
        )
        # Totaux par clause via fenêtre — évite un .collect() supplémentaire
        .withColumn("_total_nb_clause", F.sum("nb_dossiers").over(window_clause))
        .withColumn("_total_pm_clause", F.sum("pm_mrm").over(window_clause))
        .withColumn("ratio_nombre_pct",
            F.round(F.col("nb_dossiers") / F.col("_total_nb_clause") * 100, 2))
        .withColumn("ratio_pm_pct",
            F.round(F.col("pm_mrm") / F.col("_total_pm_clause") * 100, 2))
        .drop("_total_nb_clause", "_total_pm_clause")
    )


# ============================================================================
# SECTION 2 : TAUX DE CHUTE
# ============================================================================

def calculate_taux_chute(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    clause_col    : str = "CLAUSE",
) -> DataFrame:
    """
    Taux de chute agrégé par (CLAUSE, MRM_ACTION) — dossiers KEEP / STUDY uniquement.

    Définition :
        taux_chute = (PM_MRM - PM_CPT) / PM_MRM × 100
        > 0  → CPT < MRM → sous-provisionnement  (risque financier)
        < 0  → CPT > MRM → sur-provisionnement   (marge de sécurité)
        = 0  → conforme

    Les poids (poids_nb_pct, poids_pm_pct) sont calculés dans le scope de chaque
    clause via une fenêtre — aucun .collect() supplémentaire.

    Colonnes :
        CLAUSE, MRM_ACTION,
        nb_dossiers, nb_sous_prov, nb_sur_prov, nb_conforme,
        pm_mrm, pm_cpt, ecart_signe, ecart_abs,
        ecart_sous_prov, ecart_sur_prov,
        taux_chute_moyen_pct,
        poids_nb_pct, poids_pm_pct,
        pct_sous_prov, pct_sur_prov, pct_conforme

    Args:
        df_result      : DataFrame résultat du waterfall enrichi avec CLAUSE
        conclusion_col : Colonne conclusion MRM brute
        clause_col     : Colonne clause

    Returns:
        DataFrame agrégé par (CLAUSE, MRM_ACTION)
    """
    df_matched = (
        _filter_matched_keep_add_study(_with_mrm_action(df_result, conclusion_col))
        .withColumn("ecart_pm",     F.col("MRM_PM") - F.col("CPT_PM"))
        .withColumn("ecart_pm_abs", F.abs(F.col("ecart_pm")))
        .withColumn("categorie_provision",
            F.when(F.col("CPT_PM") < F.col("MRM_PM"), "SOUS")
             .when(F.col("CPT_PM") > F.col("MRM_PM"), "SUR")
             .otherwise("CONFORME")
        )
    )

    df_chute = (
        df_matched
        .groupBy(clause_col, "TYPE_CLAUSE", "MRM_ACTION")
        .agg(
            F.count("*").alias("nb_dossiers"),
            F.sum(F.when(F.col("categorie_provision") == "SOUS",     1).otherwise(0)).alias("nb_sous_prov"),
            F.sum(F.when(F.col("categorie_provision") == "SUR",      1).otherwise(0)).alias("nb_sur_prov"),
            F.sum(F.when(F.col("categorie_provision") == "CONFORME", 1).otherwise(0)).alias("nb_conforme"),
            F.sum("MRM_PM").alias("pm_mrm"),
            F.sum("CPT_PM").alias("pm_cpt"),
            F.sum("ecart_pm").alias("ecart_signe"),
            F.sum("ecart_pm_abs").alias("ecart_abs"),
            F.sum(F.when(F.col("categorie_provision") == "SOUS", F.col("ecart_pm_abs")).otherwise(0)).alias("ecart_sous_prov"),
            F.sum(F.when(F.col("categorie_provision") == "SUR",  F.col("ecart_pm_abs")).otherwise(0)).alias("ecart_sur_prov"),
        )
        # taux_chute_moyen_pct = SUM(MRM_PM - CPT_PM) / SUM(MRM_PM) × 100
        # Formule agrégée — robuste aux outliers per-dossier (évite F.avg de ratios)
        .withColumn("taux_chute_moyen_pct",
            F.round(
                F.when(F.col("pm_mrm") != 0,
                    F.col("ecart_signe") / F.col("pm_mrm") * 100
                ).otherwise(F.lit(0.0)),
                2,
            )
        )
    )

    # Poids calculés dans le scope de chaque clause via fenêtre — pas de .collect()
    window_clause = Window.partitionBy(clause_col, "TYPE_CLAUSE")

    return (
        df_chute
        .withColumn("_total_nb_clause", F.sum("nb_dossiers").over(window_clause))
        .withColumn("_total_pm_clause", F.sum("pm_mrm").over(window_clause))
        .withColumn("poids_nb_pct",
            F.round(F.col("nb_dossiers") / F.col("_total_nb_clause") * 100, 2))
        .withColumn("poids_pm_pct",
            F.round(F.col("pm_mrm") / F.col("_total_pm_clause") * 100, 2))
        .withColumn("pct_sous_prov",
            F.round(F.col("nb_sous_prov") / F.col("nb_dossiers") * 100, 2))
        .withColumn("pct_sur_prov",
            F.round(F.col("nb_sur_prov")  / F.col("nb_dossiers") * 100, 2))
        .withColumn("pct_conforme",
            F.round(F.col("nb_conforme")  / F.col("nb_dossiers") * 100, 2))
        .drop("_total_nb_clause", "_total_pm_clause")
    )


# ============================================================================
# SECTION 3 : PROVISIONNEMENT
# ============================================================================

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


# ============================================================================
# SECTION 4 : ÉCARTS DÉTAILLÉS — DISTRIBUTION PAR TRANCHES
# ============================================================================

# Tranches d'écart PM par défaut (en €)
# Utilisées pour la distribution sous/sur-provisionnement → bar chart viz
DEFAULT_ECART_TRANCHES: List[Tuple[Optional[float], Optional[float], str]] = [
    (None,    1_000,   "< 1K"),
    (1_000,   5_000,   "1K – 5K"),
    (5_000,   10_000,  "5K – 10K"),
    (10_000,  50_000,  "10K – 50K"),
    (50_000,  100_000, "50K – 100K"),
    (100_000, None,    "> 100K"),
]


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


# ============================================================================
# SECTION 5 : ORPHELINS — ANALYSE AGRÉGÉE (Power BI)
# ============================================================================
#
# Les extractions ligne par ligne (extract_cpt_only_raw / extract_mrm_missing_raw)
# sont dans core/orphans/extracts.py — non incluses dans le pipeline principal.
#
# ============================================================================

# Tranches PM pour l'analyse des orphelins (en €)
DEFAULT_PM_TRANCHES: List[Tuple[Optional[float], Optional[float], str, int]] = [
    (None,    5_000,   "< 5K",       0),
    (5_000,   20_000,  "5K – 20K",   1),
    (20_000,  50_000,  "20K – 50K",  2),
    (50_000,  100_000, "50K – 100K", 3),
    (100_000, None,    "> 100K",     4),
]


def _pm_tranche_expr(
    pm_col  : str,
    tranches: Optional[List[Tuple[Optional[float], Optional[float], str, int]]] = None,
) -> Tuple[F.Column, F.Column]:
    """
    Retourne (TRANCHE_PM, ORDRE_TRANCHE) depuis une colonne PM.

    Tranches par défaut (€) :
        < 5K | 5K – 20K | 20K – 50K | 50K – 100K | > 100K

    Args:
        pm_col   : nom de la colonne PM source
        tranches : liste de (min, max, label, ordre) — défaut : DEFAULT_PM_TRANCHES

    Returns:
        Tuple (tranche_expr, ordre_expr) — deux colonnes Spark
    """
    active       = tranches or DEFAULT_PM_TRANCHES
    pm           = F.col(pm_col)
    tranche_expr = F.lit("AUTRE")
    ordre_expr   = F.lit(99)

    for (low, high, label, ordre) in reversed(active):
        if low is None and high is not None:
            cond = pm < high
        elif low is not None and high is None:
            cond = pm >= low
        else:
            cond = (pm >= low) & (pm < high)
        tranche_expr = F.when(cond, F.lit(label)).otherwise(tranche_expr)
        ordre_expr   = F.when(cond, F.lit(ordre)).otherwise(ordre_expr)

    return tranche_expr, ordre_expr


def _mois_label_expr(date_col: str) -> F.Column:
    """
    Abréviation française du mois (Jan … Déc) depuis une colonne date.

    Args:
        date_col : nom de la colonne date source (DateType)
    """
    m = F.month(F.col(date_col))
    return (
        F.when(m == 1,  "Jan")
         .when(m == 2,  "Fév")
         .when(m == 3,  "Mar")
         .when(m == 4,  "Avr")
         .when(m == 5,  "Mai")
         .when(m == 6,  "Jun")
         .when(m == 7,  "Jul")
         .when(m == 8,  "Aoû")
         .when(m == 9,  "Sep")
         .when(m == 10, "Oct")
         .when(m == 11, "Nov")
         .otherwise("Déc")
    )


def analyse_cpt_only(
    df_result  : DataFrame,
    clause_col : str = "CLAUSE",
    date_col   : str = "CPT_D_SURVENANCE",
    pm_col     : str = "CPT_PM",
) -> DataFrame:
    """
    Analyse agrégée des dossiers CPT_ONLY par mois de survenance et tranche PM.

    Objectif : détecter les déclarations tardives (fin d'année : Oct/Nov/Déc)
    et analyser les niveaux PM des dossiers sans correspondance dans la base MRM.

    IS_FIN_ANNEE = True si MOIS_SURVENANCE ∈ {10, 11, 12}

    Dimensions de groupement :
        CLAUSE, ANNEE_SURVENANCE, MOIS_SURVENANCE, MOIS_LABEL,
        IS_FIN_ANNEE, TRANCHE_PM, ORDRE_TRANCHE

    Colonnes agrégées (PM = CPT_PM — pas d'équivalent MRM puisque ces dossiers
    n'ont aucune contrepartie MRM par définition) :
        NB_DOSSIERS, PM_CPT_TOTAL, PM_CPT_MOYEN

    Note : Pour un export ligne par ligne (ex: mapping CPT_ONLY → base MRM N+1),
    utiliser core.orphans.extracts.extract_cpt_only_raw().

    Args:
        df_result  : DataFrame résultat du waterfall enrichi avec CLAUSE
        clause_col : Colonne clause
        date_col   : Colonne date de survenance côté CPT (CPT_D_SURVENANCE)
        pm_col     : Colonne PM côté CPT (CPT_PM)

    Returns:
        DataFrame agrégé — une ligne par (CLAUSE × mois × tranche PM)
    """
    tranche_col, ordre_col = _pm_tranche_expr(pm_col)

    return (
        df_result
        .filter(F.col("TYPE_RECONCILIATION") == "CPT_ONLY")
        .withColumn("ANNEE_SURVENANCE", F.year(F.col(date_col)))
        .withColumn("MOIS_SURVENANCE",  F.month(F.col(date_col)))
        .withColumn("MOIS_LABEL",       _mois_label_expr(date_col))
        .withColumn("IS_FIN_ANNEE",     F.month(F.col(date_col)).isin(10, 11, 12))
        .withColumn("TRANCHE_PM",       tranche_col)
        .withColumn("ORDRE_TRANCHE",    ordre_col)
        .groupBy(
            F.col(clause_col).alias("CLAUSE"),
            "TYPE_CLAUSE",
            "ANNEE_SURVENANCE",
            "MOIS_SURVENANCE",
            "MOIS_LABEL",
            "IS_FIN_ANNEE",
            "TRANCHE_PM",
            "ORDRE_TRANCHE",
        )
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.round(F.sum(pm_col), 2).alias("PM_CPT_TOTAL"),
            F.round(F.avg(pm_col), 2).alias("PM_CPT_MOYEN"),
        )
        .orderBy("CLAUSE", "TYPE_CLAUSE", "ANNEE_SURVENANCE", "MOIS_SURVENANCE", "ORDRE_TRANCHE")
    )


def analyze_mrm_missing(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    clause_col    : str = "CLAUSE",
    date_col      : str = "MRM_D_SURVENANCE",
    pm_col        : str = "MRM_PM",
) -> DataFrame:
    """
    Analyse agrégée des dossiers MRM_MISSING par mois de survenance, tranche PM et consigne.

    Exclut les MRM_DELETE : consigne d'audit déjà traitée, hors périmètre de matching.
    Exclut également les MRM_ACTION nulles (conclusions non reconnues).

    IS_FIN_ANNEE = True si MOIS_SURVENANCE ∈ {10, 11, 12}

    Dimensions de groupement :
        CLAUSE, MRM_ACTION, ANNEE_SURVENANCE, MOIS_SURVENANCE, MOIS_LABEL,
        IS_FIN_ANNEE, TRANCHE_PM, ORDRE_TRANCHE

    Colonnes agrégées (PM = MRM_PM — pas d'équivalent CPT puisque ces dossiers
    n'ont aucune contrepartie CPT par définition) :
        NB_DOSSIERS, PM_MRM_TOTAL, PM_MRM_MOYEN

    Note : Pour un export ligne par ligne, utiliser
    core.orphans.extracts.extract_mrm_missing_raw().

    Args:
        df_result      : DataFrame résultat du waterfall enrichi avec CLAUSE
        conclusion_col : Colonne conclusion MRM brute (MRM_CONCLUSION après clean_mrm)
        clause_col     : Colonne clause
        date_col       : Colonne date de survenance côté MRM (MRM_D_SURVENANCE)
        pm_col         : Colonne PM côté MRM (MRM_PM)

    Returns:
        DataFrame agrégé — une ligne par (CLAUSE × MRM_ACTION × mois × tranche PM)
    """
    tranche_col, ordre_col = _pm_tranche_expr(pm_col)
    # Dimension statut inventaire (OUI/NON) si dispo — les MRM_MISSING statut NON
    # ne sont pas remontés à la direction financière (ventilation exportable).
    statut_dim = _statut_inv_dim(df_result)

    return (
        _with_mrm_action(
            df_result.filter(F.col("TYPE_RECONCILIATION") == "MRM_MISSING"),
            conclusion_col,
        )
        # MRM_DELETE exclus : consigne d'audit, hors périmètre de matching
        .filter(
            F.col("MRM_ACTION").isNotNull() &
            (F.col("MRM_ACTION") != "MRM_DELETE")
        )
        .withColumn("ANNEE_SURVENANCE", F.year(F.col(date_col)))
        .withColumn("MOIS_SURVENANCE",  F.month(F.col(date_col)))
        .withColumn("MOIS_LABEL",       _mois_label_expr(date_col))
        .withColumn("IS_FIN_ANNEE",     F.month(F.col(date_col)).isin(10, 11, 12))
        .withColumn("TRANCHE_PM",       tranche_col)
        .withColumn("ORDRE_TRANCHE",    ordre_col)
        .groupBy(
            F.col(clause_col).alias("CLAUSE"),
            "TYPE_CLAUSE",
            *statut_dim,
            "MRM_ACTION",
            "ANNEE_SURVENANCE",
            "MOIS_SURVENANCE",
            "MOIS_LABEL",
            "IS_FIN_ANNEE",
            "TRANCHE_PM",
            "ORDRE_TRANCHE",
        )
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.round(F.sum(pm_col), 2).alias("PM_MRM_TOTAL"),
            F.round(F.avg(pm_col), 2).alias("PM_MRM_MOYEN"),
        )
        .orderBy("CLAUSE", "TYPE_CLAUSE", *statut_dim, "MRM_ACTION", "ANNEE_SURVENANCE", "MOIS_SURVENANCE", "ORDRE_TRANCHE")
    )


# ============================================================================
# ENRICHISSEMENT DU RÉSULTAT : consigne reformatée + tag des orphelins
# ============================================================================

def _inventory_year() -> Optional[int]:
    """Année d'inventaire dérivée de DATE_INVENTAIRE ('dd/MM/yyyy'). None si 'auto'."""
    try:
        return int(str(DATE_INVENTAIRE).split("/")[-1])
    except (ValueError, AttributeError):
        return None


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


# ============================================================================
# VENTILATION CPT_ONLY (synthèse main — survenance × garantie, PM décroissant)
# ============================================================================

def ventilate_cpt_only(
    df_result   : DataFrame,
    date_col    : str = "CPT_D_SURVENANCE",
    garantie_col: str = "CPT_GARANTIE",
    pm_col      : str = "CPT_PM",
) -> DataFrame:
    """
    Ventile les dossiers CPT_ONLY définitifs par (année, mois de survenance,
    garantie), triés par PM total décroissant.

    Vue de pilotage : où se concentre la PM des dossiers CPT sans contrepartie
    MRM (ni à l'inventaire courant, ni dans le N+1).

    Le JOUR (= jour du mois de survenance) est résumé par croisement :
        JOUR_MODE = jour le plus fréquent parmi les dossiers du croisement
        JOUR_MIN  = jour le plus précoce
    Utile pour repérer un effet "bloc" (ex. déclarations groupées un 31).

    Colonnes : ANNEE_SURVENANCE, MOIS_SURVENANCE, GARANTIE,
               NB_DOSSIERS, PM_CPT_TOTAL, PM_CPT_MOYEN, JOUR_MODE, JOUR_MIN

    Args:
        df_result    : DataFrame résultat (sortie waterfall + recovery)
        date_col     : Colonne date de survenance CPT
        garantie_col : Colonne garantie CPT
        pm_col       : Colonne PM CPT

    Returns:
        DataFrame ventilé, trié PM décroissant.

    Note : F.mode() requiert Spark 3.4+ (Databricks Runtime 13+).
    """
    return (
        df_result
        .filter(F.col("TYPE_RECONCILIATION") == "CPT_ONLY")
        .withColumn("_JOUR", F.dayofmonth(F.col(date_col)))
        .groupBy(
            F.year(F.col(date_col)).alias("ANNEE_SURVENANCE"),
            F.month(F.col(date_col)).alias("MOIS_SURVENANCE"),
            F.col(garantie_col).alias("GARANTIE"),
        )
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.round(F.coalesce(F.sum(pm_col), F.lit(0.0)), 2).alias("PM_CPT_TOTAL"),
            F.round(F.coalesce(F.avg(pm_col), F.lit(0.0)), 2).alias("PM_CPT_MOYEN"),
            F.mode(F.col("_JOUR")).alias("JOUR_MODE"),
            F.min(F.col("_JOUR")).alias("JOUR_MIN"),
        )
        .orderBy(F.desc("PM_CPT_TOTAL"))
    )


# ============================================================================
# ÉTUDE DU PROVISIONNEMENT (matchés hors "à supprimer") — stats pour graphiques
# ============================================================================

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


# ============================================================================
# ANALYSES PAS-À-PAS (mono-client SODEXO — sans dimension CLAUSE)
#
# Fonctions autonomes appelées comme ÉTAPES dans main.py après le matching.
# Chacune renvoie un DataFrame .show()-able pour vérification manuelle, avant
# agrégation ultérieure dans un pipeline global.
#
# Univers "matchés" de référence = matchs légitimes de l'inventaire courant
# (MATCH_LABELS) + récupérés N+1 réels (CPT_LATE, contrepartie MRM existante).
# EXCLUS partout des comparaisons PM : MRM_MISSING, CPT_ONLY définitifs et
# observations tardives IT (CPT_OBS_TARDIVE) — ces dossiers n'ont pas matché.
# ============================================================================

# Colonnes d'identité MRM (alignées sur mrm_dup_keys) pour compter les dossiers
# MRM distincts et repérer le fan-out (1 MRM matché par plusieurs CPT).
_MRM_ID_COLS = ["MRM_IDCORP", "MRM_D_NAISSANCE", "MRM_D_SURVENANCE", "MRM_GARANTIE"]


def _matched_universe() -> List[str]:
    """Matchs légitimes + récupérés N+1 réels (= dossiers ayant une contrepartie
    MRM qui a matché). N'inclut PAS CPT_OBS_TARDIVE (anomalie, jamais matchée)."""
    return list(MATCH_LABELS) + ["CPT_LATE"]


def _mrm_identity(df: DataFrame) -> F.Column:
    """Identifiant d'un dossier MRM = concat des colonnes d'identité présentes."""
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in _MRM_ID_COLS if c in df.columns]
    return F.concat_ws("|", *cols) if cols else F.lit("∅")


# ----------------------------------------------------------------------------
# DIAGNOSTIC — écart MRM clean vs synthèse mrm_nb (fan-out CPT)
# ----------------------------------------------------------------------------

def diagnose_mrm_fanout(
    df_result   : DataFrame,
    df_mrm_clean : Optional[DataFrame] = None,
    top         : int = 30,
) -> DataFrame:
    """
    Explique l'écart entre le MRM nettoyé (ex: 5121) et le mrm_nb de la synthèse
    (ex: 5209). Hypothèse : fan-out CPT — une même ligne MRM matchée par plusieurs
    lignes CPT est comptée plusieurs fois côté "matchés" (nb_match).

    Affiche un récapitulatif chiffré :
        - lignes matchées (nb_match, côté CPT après join)
        - dossiers MRM distincts matchés
        - sur-comptage = lignes matchées − MRM distincts (= fan-out CPT)
        - [si df_mrm_clean fourni] réconciliation au total MRM en entrée

    Renvoie le DÉTAIL des dossiers MRM matchés par > 1 CPT (à inspecter).

    Aucune correction de mrm_nb n'est faite ici : on confirme d'abord la cause.
    """
    matched = df_result.filter(F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS)))
    matched = matched.withColumn("_MRM_ID", _mrm_identity(matched))

    nb_matched_rows = matched.count()
    nb_mrm_distinct = matched.select("_MRM_ID").distinct().count()
    fanout_overcount = nb_matched_rows - nb_mrm_distinct

    print("\n[DIAG fan-out] écart MRM clean ↔ synthèse mrm_nb")
    print(f"  lignes matchées (nb_match, côté CPT)   : {nb_matched_rows:,}")
    print(f"  dossiers MRM distincts matchés         : {nb_mrm_distinct:,}")
    print(f"  → sur-comptage fan-out CPT (≈ écart)   : {fanout_overcount:,}")

    if df_mrm_clean is not None:
        n_mrm_in = df_mrm_clean.count()
        n_mrm_id = df_mrm_clean.withColumn("_MRM_ID", _mrm_identity(df_mrm_clean)) \
                               .select("_MRM_ID").distinct().count()
        print(f"  MRM clean en entrée (lignes)           : {n_mrm_in:,}")
        print(f"  MRM clean en entrée (identités dist.)  : {n_mrm_id:,}")
        print(f"  → doublons d'identité MRM en entrée    : {n_mrm_in - n_mrm_id:,}")

    # Détail : dossiers MRM matchés par plusieurs CPT, PM décroissante.
    nom_col = "MRM_NOM_PRENOM" if "MRM_NOM_PRENOM" in matched.columns else None
    detail = (
        matched
        .groupBy("_MRM_ID")
        .agg(
            F.count("*").alias("NB_CPT"),
            F.round(F.first("MRM_PM"), 2).alias("MRM_PM"),
            F.collect_set("TYPE_RECONCILIATION").alias("ETAPES"),
            *( [F.first(nom_col).alias("NOM_PRENOM")] if nom_col else [] ),
        )
        .filter(F.col("NB_CPT") > 1)
        .orderBy(F.desc("NB_CPT"), F.desc("MRM_PM"))
    )
    print(f"  dossiers MRM en fan-out (NB_CPT > 1) — top {top} :")
    return detail


# ----------------------------------------------------------------------------
# TAUX DE CHUTE (matchés + récupérés N+1 ; obs tardives & orphelins EXCLUS)
# ----------------------------------------------------------------------------

def analyze_taux_chute(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
) -> DataFrame:
    """
    Taux de chute par consigne (KEEP / ADD / STUDY) sur les dossiers MATCHÉS
    uniquement (MATCH_LABELS + récupérés N+1). Les MRM_MISSING, CPT_ONLY et
    observations tardives IT n'ont pas de contrepartie matchée → exclus.

    taux_chute = Σ(MRM_PM − CPT_PM) / Σ(MRM_PM) × 100   (formule agrégée)
        > 0 → sous-provisionnement (CPT < MRM, risque)
        < 0 → sur-provisionnement  (CPT > MRM, marge)

    Colonnes : MRM_ACTION, nb_dossiers, nb_sous, nb_sur, nb_conforme,
               pm_mrm, pm_cpt, ecart_signe, taux_chute_pct.
    """
    df = (
        _with_mrm_action(df_result, conclusion_col)
        .filter(F.col("TYPE_RECONCILIATION").isin(_matched_universe()))
        .filter(F.col("MRM_ACTION").isin("MRM_KEEP", "MRM_ADD", "MRM_STUDY"))
        .withColumn("_ecart", F.coalesce(F.col("MRM_PM"), F.lit(0.0))
                            - F.coalesce(F.col("CPT_PM"), F.lit(0.0)))
    )
    return (
        df.groupBy("MRM_ACTION")
        .agg(
            F.count("*").alias("nb_dossiers"),
            F.sum(F.when(F.col("_ecart") > 0, 1).otherwise(0)).alias("nb_sous"),
            F.sum(F.when(F.col("_ecart") < 0, 1).otherwise(0)).alias("nb_sur"),
            F.sum(F.when(F.col("_ecart") == 0, 1).otherwise(0)).alias("nb_conforme"),
            F.round(F.sum("MRM_PM"), 2).alias("pm_mrm"),
            F.round(F.sum("CPT_PM"), 2).alias("pm_cpt"),
            F.round(F.sum("_ecart"), 2).alias("ecart_signe"),
        )
        .withColumn("taux_chute_pct",
            F.round(F.when(F.col("pm_mrm") != 0,
                           F.col("ecart_signe") / F.col("pm_mrm") * 100).otherwise(0.0), 2))
        .orderBy(F.desc("nb_dossiers"))
    )


# ----------------------------------------------------------------------------
# CONSIGNES × NIVEAUX DE PM (analyse pas-à-pas)
# ----------------------------------------------------------------------------

def analyze_consignes_pm(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    pm_col        : str = "MRM_PM",
) -> DataFrame:
    """
    Analyse pas-à-pas des consignes (KEEP / ADD / STUDY) croisées au niveau de PM,
    sur les dossiers MATCHÉS uniquement (MATCH_LABELS + récupérés N+1).

    Pour chaque consigne, ventile par catégorie de provisionnement
    (SOUS / SUR / CONFORME) et par tranche de PM MRM (réutilise _pm_tranche_expr).

    Colonnes : MRM_ACTION, CATEGORIE_PROVISION, TRANCHE_PM, ORDRE_TRANCHE,
               nb_dossiers, pm_mrm, pm_cpt, ecart_signe, taux_chute_pct,
               pct_nb_consigne (poids du croisement dans la consigne).
    """
    tranche_col, ordre_col = _pm_tranche_expr(pm_col)
    pm_mrm = F.coalesce(F.col("MRM_PM"), F.lit(0.0))
    pm_cpt = F.coalesce(F.col("CPT_PM"), F.lit(0.0))

    df = (
        _with_mrm_action(df_result, conclusion_col)
        .filter(F.col("TYPE_RECONCILIATION").isin(_matched_universe()))
        .filter(F.col("MRM_ACTION").isin("MRM_KEEP", "MRM_ADD", "MRM_STUDY"))
        .withColumn("CATEGORIE_PROVISION",
            F.when(pm_cpt < pm_mrm, "SOUS_PROVISIONNE")
             .when(pm_cpt > pm_mrm, "SUR_PROVISIONNE")
             .otherwise("CONFORME"))
        .withColumn("TRANCHE_PM",    tranche_col)
        .withColumn("ORDRE_TRANCHE", ordre_col)
        .withColumn("_ecart", pm_mrm - pm_cpt)
    )

    window_consigne = Window.partitionBy("MRM_ACTION")
    return (
        df.groupBy("MRM_ACTION", "CATEGORIE_PROVISION", "TRANCHE_PM", "ORDRE_TRANCHE")
        .agg(
            F.count("*").alias("nb_dossiers"),
            F.round(F.sum("MRM_PM"), 2).alias("pm_mrm"),
            F.round(F.sum("CPT_PM"), 2).alias("pm_cpt"),
            F.round(F.sum("_ecart"), 2).alias("ecart_signe"),
        )
        .withColumn("taux_chute_pct",
            F.round(F.when(F.col("pm_mrm") != 0,
                           F.col("ecart_signe") / F.col("pm_mrm") * 100).otherwise(0.0), 2))
        .withColumn("pct_nb_consigne",
            F.round(F.col("nb_dossiers") / F.sum("nb_dossiers").over(window_consigne) * 100, 2))
        .orderBy("MRM_ACTION", "CATEGORIE_PROVISION", "ORDRE_TRANCHE")
    )


# ----------------------------------------------------------------------------
# SUIVI GLOBAL DES CONSIGNES + PM (une ligne par consigne)
# ----------------------------------------------------------------------------

# Ordre d'affichage et libellés des consignes.
_CONSIGNE_ORDRE = {"MRM_KEEP": 0, "MRM_STUDY": 1, "MRM_ADD": 2, "MRM_DELETE": 3}


def analyze_suivi_consignes_global(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
) -> DataFrame:
    """
    Suivi GLOBAL des consignes MRM accompagné des PM — une ligne par consigne
    (MRM_KEEP / MRM_STUDY / MRM_ADD / MRM_DELETE).

    Univers par consigne = dossiers matchés à l'inventaire (MATCH_LABELS) +
    orphelins MRM portant cette consigne (MRM_MISSING pour KEEP/STUDY/ADD,
    MRM_DELETE pour DELETE). Les CPT_LATE (autre inventaire), CPT_OBS_TARDIVE et
    CPT_ONLY (pas de consigne MRM) sont hors univers.

    Conformité :
        KEEP / STUDY / ADD → conforme = retrouvé au compte (matché)
        DELETE             → conforme = absent du compte (orphelin, donc supprimé)

    Les PM (MRM, CPT, écart, taux de chute) ne portent que sur les dossiers
    MATCHÉS. Pour DELETE, ces matchés sont justement les consignes NON suivies
    (PM toujours présente) → taux de chute non pertinent (null).

    Colonnes : MRM_ACTION, ORDRE, nb_total, nb_matches, nb_orphelins,
               nb_conformes, pct_conformite, nb_pm_nulle, nb_pm_non_nulle,
               pm_mrm, pm_cpt, ecart, taux_chute_pct.
    """
    is_m = F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS))
    df = (
        _with_mrm_action(df_result, conclusion_col)
        .filter(F.col("MRM_ACTION").isNotNull())
        .filter(is_m | F.col("TYPE_RECONCILIATION").isin("MRM_MISSING", "MRM_DELETE"))
    )

    agg = (
        df.groupBy("MRM_ACTION")
        .agg(
            F.count("*").alias("nb_total"),
            F.sum(F.when(is_m, 1).otherwise(0)).alias("nb_matches"),
            F.sum(F.when(is_m & F.col("MRM_PM").isNotNull() & (F.col("MRM_PM") != 0), 1)
                   .otherwise(0)).alias("nb_pm_non_nulle"),
            F.round(F.sum(F.when(is_m, F.col("MRM_PM")).otherwise(0.0)), 2).alias("pm_mrm"),
            F.round(F.sum(F.when(is_m, F.col("CPT_PM")).otherwise(0.0)), 2).alias("pm_cpt"),
        )
    )

    ordre_expr = F.lit(99)
    for action, idx in _CONSIGNE_ORDRE.items():
        ordre_expr = F.when(F.col("MRM_ACTION") == action, idx).otherwise(ordre_expr)

    is_delete = F.col("MRM_ACTION") == "MRM_DELETE"
    return (
        agg
        .withColumn("ORDRE", ordre_expr)
        .withColumn("nb_orphelins", F.col("nb_total") - F.col("nb_matches"))
        .withColumn("nb_conformes",
            F.when(is_delete, F.col("nb_orphelins")).otherwise(F.col("nb_matches")))
        .withColumn("pct_conformite",
            F.round(F.col("nb_conformes") / F.col("nb_total") * 100, 2))
        .withColumn("nb_pm_nulle", F.col("nb_matches") - F.col("nb_pm_non_nulle"))
        .withColumn("ecart", F.round(F.col("pm_mrm") - F.col("pm_cpt"), 2))
        .withColumn("taux_chute_pct",
            F.when(is_delete | (F.col("pm_mrm") == 0), None)
             .otherwise(F.round(F.col("ecart") / F.col("pm_mrm") * 100, 2)))
        .select(
            "MRM_ACTION", "ORDRE", "nb_total", "nb_matches", "nb_orphelins",
            "nb_conformes", "pct_conformite", "nb_pm_nulle", "nb_pm_non_nulle",
            "pm_mrm", "pm_cpt", "ecart", "taux_chute_pct",
        )
        .orderBy("ORDRE")
    )


# ----------------------------------------------------------------------------
# CONSIGNES "À SUPPRIMER" NON SUIVIES (MRM_DELETE pourtant matché)
# ----------------------------------------------------------------------------

def analyze_delete_non_suivies(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    pm_col        : str = "MRM_PM",
) -> DataFrame:
    """
    Consignes "à supprimer" NON suivies : dossiers dont la consigne MRM est
    MRM_DELETE mais qui ont matché un CPT (TYPE_RECONCILIATION ∈ MATCH_LABELS).
    La PM MRM aurait dû être supprimée mais elle est toujours présente au compte.

    Analyse séparée (volumétrie + niveau de PM par tranche) : la consigne n'a pas
    été appliquée → enjeu financier à remonter.

    Colonnes : TRANCHE_PM, ORDRE_TRANCHE, nb_dossiers, pm_mrm_non_supprimee,
               pm_cpt, pct_nb.
    """
    tranche_col, ordre_col = _pm_tranche_expr(pm_col)
    df = (
        _with_mrm_action(df_result, conclusion_col)
        .filter(
            (F.col("MRM_ACTION") == "MRM_DELETE")
            & F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS))
        )
        .withColumn("TRANCHE_PM",    tranche_col)
        .withColumn("ORDRE_TRANCHE", ordre_col)
    )
    w = Window.partitionBy()
    return (
        df.groupBy("TRANCHE_PM", "ORDRE_TRANCHE")
        .agg(
            F.count("*").alias("nb_dossiers"),
            F.round(F.sum("MRM_PM"), 2).alias("pm_mrm_non_supprimee"),
            F.round(F.sum("CPT_PM"), 2).alias("pm_cpt"),
        )
        .withColumn("pct_nb",
            F.round(F.col("nb_dossiers") / F.sum("nb_dossiers").over(w) * 100, 2))
        .orderBy("ORDRE_TRANCHE")
    )


# ----------------------------------------------------------------------------
# OBSERVATIONS TARDIVES IT (anomalies — présentées hors métriques)
# ----------------------------------------------------------------------------

def analyze_obs_tardives(
    df_result   : DataFrame,
    date_col    : str = "CPT_D_SURVENANCE",
    garantie_col: str = "CPT_GARANTIE",
    pm_col      : str = "CPT_PM",
) -> DataFrame:
    """
    Ventile les observations tardives IT (TYPE_RECONCILIATION = CPT_OBS_TARDIVE)
    par (année, mois de survenance, garantie), PM CPT décroissante.

    Ce ne sont PAS des anomalies : sinistres dont l'arrêt s'est clos avant la date
    d'inventaire du MRM suivant → logiquement non retrouvés. Exclus des taux et des
    comparaisons de PM, mais on présente ici leur volumétrie et leur PM compte.

    Colonnes : ANNEE_SURVENANCE, MOIS_SURVENANCE, GARANTIE,
               NB_DOSSIERS, PM_CPT_TOTAL, PM_CPT_MOYEN.
    """
    return (
        df_result
        .filter(F.col("TYPE_RECONCILIATION") == OBS_TARDIVE_LABEL)
        .groupBy(
            F.year(F.col(date_col)).alias("ANNEE_SURVENANCE"),
            F.month(F.col(date_col)).alias("MOIS_SURVENANCE"),
            F.col(garantie_col).alias("GARANTIE"),
        )
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.round(F.coalesce(F.sum(pm_col), F.lit(0.0)), 2).alias("PM_CPT_TOTAL"),
            F.round(F.coalesce(F.avg(pm_col), F.lit(0.0)), 2).alias("PM_CPT_MOYEN"),
        )
        .orderBy(F.desc("PM_CPT_TOTAL"))
    )


# ============================================================================
# POINT D'ENTRÉE : RUN FULL ANALYSIS
# ============================================================================

def run_full_analysis(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    clause_col    : str = "CLAUSE",
) -> Dict[str, DataFrame]:
    """
    Exécute toutes les analyses en une passe.
    Retourne le dict prêt pour export_for_powerbi().

    df_result doit être persisté avant l'appel (api.py s'en charge).

    Tables produites (clés alignées avec out_cfg.as_table_map()) :
        synthese_consignes   — audit conformité MRM par (CLAUSE, MRM_ACTION, RESULTAT_AUDIT)
        stats_reconciliation — distribution par (CLAUSE, MRM_ACTION, TYPE_RECONCILIATION)
        taux_chute           — taux de chute par (CLAUSE, MRM_ACTION)
        provisionnement      — catégories par (CLAUSE, MRM_ACTION, CATEGORIE_PROVISION)
        ecarts_tranches      — distribution par (CLAUSE, MRM_ACTION, CATEGORIE, TRANCHE)
        analyse_cpt_only     — CPT_ONLY agrégés par mois + tranche PM        (Power BI)
        analyse_mrm_missing  — MRM_MISSING agrégés par mois + tranche PM + consigne (Power BI)

    Args:
        df_result      : DataFrame résultat du waterfall (persisté + enrichi CLAUSE)
        conclusion_col : Colonne conclusion MRM brute (MRM_CONCLUSION après clean_mrm)
        clause_col     : Colonne clause

    Returns:
        Dict {clé_logique: DataFrame} — 7 tables prêtes pour Power BI
    """
    synthese_consignes, _ = analyze_suivi_consignes(df_result, conclusion_col, clause_col)

    return {
        "synthese_consignes"   : synthese_consignes,
        "stats_reconciliation" : analyze_consignes_ratios(df_result, conclusion_col, clause_col),
        "taux_chute"           : calculate_taux_chute(df_result, conclusion_col, clause_col),
        "provisionnement"      : analyze_provisionnement(df_result, conclusion_col, clause_col),
        "ecarts_tranches"      : analyze_ecarts_tranches(df_result, conclusion_col, clause_col=clause_col),
        # ── Analyses orphelins agrégées (Power BI) ──────────────────────────
        "analyse_cpt_only"     : analyse_cpt_only(df_result, clause_col),
        "analyse_mrm_missing"  : analyze_mrm_missing(df_result, conclusion_col, clause_col),
    }
