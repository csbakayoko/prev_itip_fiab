"""Taux de chute CPT/MRM, ventilé par clause (agrégé Power BI + synthèse)."""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import Dict, List, Optional, Tuple

from modules.analysis.helpers import (
    _with_mrm_action, _filter_matched_keep_add_study, _matched_universe,
)


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


def analyze_taux_chute_par_clause(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
) -> DataFrame:
    """
    Taux de chute GLOBAL par clause — une ligne par (CLAUSE, TYPE_CLAUSE),
    toutes consignes KEEP / ADD / STUDY confondues.

    Même univers (MATCHÉS + CPT_LATE) et même formule agrégée que le taux de
    chute global de la synthèse ⇒ chaque ligne est exactement l'agrégat des
    consignes de la clause dans la table taux_chute, et l'agrégat de toutes
    les lignes (Σ ecart_signe / Σ pm_mrm) redonne le taux de chute global.

    Colonnes : CLAUSE, TYPE_CLAUSE, nb_dossiers, nb_sous, nb_sur, nb_conforme,
               pm_mrm, pm_cpt, ecart_signe, taux_chute_pct, poids_pm_pct.
    """
    df = (
        _filter_matched_keep_add_study(_with_mrm_action(df_result, conclusion_col))
        .withColumn("_ecart", F.coalesce(F.col("MRM_PM"), F.lit(0.0))
                            - F.coalesce(F.col("CPT_PM"), F.lit(0.0)))
    )
    agg = (
        df.groupBy("CLAUSE", "TYPE_CLAUSE")
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
    )
    # Poids de la clause dans la PM MRM totale : montre que le global est la
    # moyenne PONDÉRÉE des taux par clause (pas leur somme).
    w = Window.partitionBy()
    return (
        agg.withColumn("poids_pm_pct",
            F.round(F.col("pm_mrm") / F.sum("pm_mrm").over(w) * 100, 2))
        .orderBy(F.desc("pm_mrm"))
    )


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

    Ventilé par (CLAUSE, TYPE_CLAUSE) : une ligne par consigne et par clause.

    Colonnes : CLAUSE, TYPE_CLAUSE, MRM_ACTION, nb_dossiers, nb_sous, nb_sur,
               nb_conforme, pm_mrm, pm_cpt, ecart_signe, taux_chute_pct.
    """
    df = (
        _with_mrm_action(df_result, conclusion_col)
        .filter(F.col("TYPE_RECONCILIATION").isin(_matched_universe()))
        .filter(F.col("MRM_ACTION").isin("MRM_KEEP", "MRM_ADD", "MRM_STUDY"))
        .withColumn("_ecart", F.coalesce(F.col("MRM_PM"), F.lit(0.0))
                            - F.coalesce(F.col("CPT_PM"), F.lit(0.0)))
    )
    return (
        df.groupBy("CLAUSE", "TYPE_CLAUSE", "MRM_ACTION")
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
        .orderBy("CLAUSE", "TYPE_CLAUSE", F.desc("nb_dossiers"))
    )

