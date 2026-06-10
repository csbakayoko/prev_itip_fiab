"""Suivi des consignes MRM (audit conformité, ratios, PM)."""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import Dict, List, Optional, Tuple

from config import MATCH_LABELS, RECUP_NON_LABEL
from modules.analysis.helpers import (
    _with_mrm_action, _statut_inv_dim, _pm_tranche_expr,
    _matched_universe, _CONSIGNE_ORDRE,
)


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

    Univers = MATCH_LABELS + MRM_MISSING + MRM_DELETE (cf. METRIQUES.md §5).
    Les CPT_LATE (consigne issue d'un autre inventaire) et CPT_RECUP_NON
    (repêchés via statut NON, hors métriques) portent une conclusion MRM
    enrichie mais sont HORS univers conformité → exclus explicitement, sinon
    ils seraient comptés à tort en NON_CONFORME.

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
        .filter(is_matched | F.col("TYPE_RECONCILIATION").isin("MRM_MISSING", "MRM_DELETE"))
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
        # CPT_RECUP_NON exclus : repêchés via un MRM statut NON (PM MRM = 0),
        # hors métriques par construction — leur conclusion enrichie ne doit
        # pas peser dans la distribution ni dans les totaux par clause.
        .filter(F.col("TYPE_RECONCILIATION") != RECUP_NON_LABEL)
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

    Ventilé par (CLAUSE, TYPE_CLAUSE) ; pct_nb_consigne est le poids du
    croisement dans la consigne, dans le scope de chaque clause.

    Colonnes : CLAUSE, TYPE_CLAUSE, MRM_ACTION, CATEGORIE_PROVISION, TRANCHE_PM,
               ORDRE_TRANCHE, nb_dossiers, pm_mrm, pm_cpt, ecart_signe,
               taux_chute_pct, pct_nb_consigne.
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

    window_consigne = Window.partitionBy("CLAUSE", "TYPE_CLAUSE", "MRM_ACTION")
    return (
        df.groupBy("CLAUSE", "TYPE_CLAUSE", "MRM_ACTION", "CATEGORIE_PROVISION", "TRANCHE_PM", "ORDRE_TRANCHE")
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
        .orderBy("CLAUSE", "TYPE_CLAUSE", "MRM_ACTION", "CATEGORIE_PROVISION", "ORDRE_TRANCHE")
    )


def analyze_suivi_consignes_global(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    par_clause    : bool = True,
) -> DataFrame:
    """
    Suivi des consignes MRM accompagné des PM — une ligne par consigne
    (MRM_KEEP / MRM_STUDY / MRM_ADD / MRM_DELETE).

    Deux univers DISTINCTS par consigne (cf. METRIQUES.md §4.2 et §5) :
      - CONFORMITÉ (nb_total, nb_conformes, pct) : matchés à l'inventaire
        (MATCH_LABELS) + orphelins MRM portant la consigne (MRM_MISSING pour
        KEEP/STUDY/ADD, MRM_DELETE pour DELETE).
      - PM / CHUTE (nb_pm_univers, pm_mrm, pm_cpt, écart, taux) : matchés +
        récupérés N+1 (CPT_LATE) — le MÊME univers que le taux de chute global
        de la synthèse et que la table taux_chute ⇒ les chiffres se
        réconcilient exactement (Σ par consigne = global).

    Conformité :
        KEEP / STUDY / ADD → conforme = retrouvé au compte (matché)
        DELETE             → conforme = absent du compte (orphelin, donc
                             supprimé) ; taux de chute non pertinent (null).

    par_clause=True  → une ligne par (CLAUSE, TYPE_CLAUSE, consigne).
    par_clause=False → une ligne par consigne, toutes clauses confondues
                       (onglet suivi_consignes_global).

    Colonnes : [CLAUSE, TYPE_CLAUSE,] MRM_ACTION, ORDRE, nb_total, nb_matches,
               nb_orphelins, nb_conformes, pct_conformite, nb_pm_univers,
               nb_pm_nulle, nb_pm_non_nulle, pm_mrm, pm_cpt, ecart,
               taux_chute_pct.
    """
    is_m    = F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS))
    is_pm   = F.col("TYPE_RECONCILIATION").isin(_matched_universe())   # + CPT_LATE
    is_orph = F.col("TYPE_RECONCILIATION").isin("MRM_MISSING", "MRM_DELETE")
    df = (
        _with_mrm_action(df_result, conclusion_col)
        .filter(F.col("MRM_ACTION").isNotNull())
        .filter(is_pm | is_orph)
    )

    group = (["CLAUSE", "TYPE_CLAUSE"] if par_clause else []) + ["MRM_ACTION"]
    agg = (
        df.groupBy(*group)
        .agg(
            # Univers conformité (matchés + orphelins) — les CPT_LATE n'y sont pas.
            F.sum(F.when(is_m | is_orph, 1).otherwise(0)).alias("nb_total"),
            F.sum(F.when(is_m, 1).otherwise(0)).alias("nb_matches"),
            # Univers PM / chute (matchés + CPT_LATE).
            F.sum(F.when(is_pm, 1).otherwise(0)).alias("nb_pm_univers"),
            F.sum(F.when(is_pm & F.col("MRM_PM").isNotNull() & (F.col("MRM_PM") != 0), 1)
                   .otherwise(0)).alias("nb_pm_non_nulle"),
            F.round(F.sum(F.when(is_pm, F.col("MRM_PM")).otherwise(0.0)), 2).alias("pm_mrm"),
            F.round(F.sum(F.when(is_pm, F.col("CPT_PM")).otherwise(0.0)), 2).alias("pm_cpt"),
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
        .withColumn("nb_pm_nulle", F.col("nb_pm_univers") - F.col("nb_pm_non_nulle"))
        .withColumn("ecart", F.round(F.col("pm_mrm") - F.col("pm_cpt"), 2))
        .withColumn("taux_chute_pct",
            F.when(is_delete | (F.col("pm_mrm") == 0), None)
             .otherwise(F.round(F.col("ecart") / F.col("pm_mrm") * 100, 2)))
        .select(
            *group, "ORDRE", "nb_total", "nb_matches", "nb_orphelins",
            "nb_conformes", "pct_conformite", "nb_pm_univers", "nb_pm_nulle",
            "nb_pm_non_nulle", "pm_mrm", "pm_cpt", "ecart", "taux_chute_pct",
        )
        .orderBy(*group[:-1], "ORDRE")
    )


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

    Ventilé par (CLAUSE, TYPE_CLAUSE) ; pct_nb est le poids de la tranche dans
    le scope de chaque clause.

    Colonnes : CLAUSE, TYPE_CLAUSE, TRANCHE_PM, ORDRE_TRANCHE, nb_dossiers,
               pm_mrm_non_supprimee, pm_cpt, pct_nb.
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
    w = Window.partitionBy("CLAUSE", "TYPE_CLAUSE")
    return (
        df.groupBy("CLAUSE", "TYPE_CLAUSE", "TRANCHE_PM", "ORDRE_TRANCHE")
        .agg(
            F.count("*").alias("nb_dossiers"),
            F.round(F.sum("MRM_PM"), 2).alias("pm_mrm_non_supprimee"),
            F.round(F.sum("CPT_PM"), 2).alias("pm_cpt"),
        )
        .withColumn("pct_nb",
            F.round(F.col("nb_dossiers") / F.sum("nb_dossiers").over(w) * 100, 2))
        .orderBy("CLAUSE", "TYPE_CLAUSE", "ORDRE_TRANCHE")
    )

