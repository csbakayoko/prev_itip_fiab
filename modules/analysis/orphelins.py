"""Orphelins : CPT_ONLY, MRM_MISSING, ventilation, obs tardives IT."""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import Dict, List, Optional, Tuple

from config import OBS_TARDIVE_LABEL, RECUP_NON_LABEL
from modules.analysis.helpers import (
    _with_mrm_action, _statut_inv_dim, _pm_tranche_expr, _mois_label_expr,
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

    Ventilé par (CLAUSE, TYPE_CLAUSE) — dérivées du préfixe CPT pour ces
    dossiers sans contrepartie MRM.

    Colonnes : CLAUSE, TYPE_CLAUSE, ANNEE_SURVENANCE, MOIS_SURVENANCE, GARANTIE,
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
            "CLAUSE",
            "TYPE_CLAUSE",
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

    Ventilé par (CLAUSE, TYPE_CLAUSE) — dérivées du préfixe CPT pour ces
    dossiers sans contrepartie MRM.

    Colonnes : CLAUSE, TYPE_CLAUSE, ANNEE_SURVENANCE, MOIS_SURVENANCE, GARANTIE,
               NB_DOSSIERS, PM_CPT_TOTAL, PM_CPT_MOYEN.
    """
    return (
        df_result
        .filter(F.col("TYPE_RECONCILIATION") == OBS_TARDIVE_LABEL)
        .groupBy(
            "CLAUSE",
            "TYPE_CLAUSE",
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


def analyze_recup_statut_non(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    clause_col    : str = "CLAUSE",
    pm_cpt_col    : str = "CPT_PM",
    pm_mrm_col    : str = "MRM_PM",
) -> DataFrame:
    """
    Analyse dédiée des CPT_ONLY récupérés via un MRM statut NON (CPT_RECUP_NON).

    Ces dossiers prouvent qu'une contrepartie MRM existe (statut NON → PM MRM = 0,
    non remontée à la direction financière). Le repêchage résout une anomalie
    (le CPT n'est plus orphelin) mais SANS valeur MRM comparable → ils sont EXCLUS
    de toutes les métriques de valeur (chute, niveaux de PM, couverture) et
    présentés ici à part.

    Ventilé par (CLAUSE, TYPE_CLAUSE, MRM_ACTION) + LATE_KEY (étape de la
    cascade ayant permis le repêchage, ex. MATCH_EXACT / MATCH_RECHUTE, si
    présente). PM_CPT = enjeu compte récupéré ;
    NB_PM_MRM_NON_NULLE contrôle l'hypothèse « PM MRM = 0 » (doit valoir 0).

    Colonnes : CLAUSE, TYPE_CLAUSE, MRM_ACTION, [LATE_KEY], NB_DOSSIERS,
               PM_CPT_TOTAL, PM_MRM_TOTAL, NB_PM_MRM_NON_NULLE.
    """
    key_dim = ["LATE_KEY"] if "LATE_KEY" in df_result.columns else []
    return (
        _with_mrm_action(
            df_result.filter(F.col("TYPE_RECONCILIATION") == RECUP_NON_LABEL),
            conclusion_col,
        )
        .groupBy(clause_col, "TYPE_CLAUSE", "MRM_ACTION", *key_dim)
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.round(F.coalesce(F.sum(pm_cpt_col), F.lit(0.0)), 2).alias("PM_CPT_TOTAL"),
            F.round(F.coalesce(F.sum(pm_mrm_col), F.lit(0.0)), 2).alias("PM_MRM_TOTAL"),
            F.sum(F.when(F.col(pm_mrm_col).isNotNull() & (F.col(pm_mrm_col) != 0), 1)
                   .otherwise(0)).alias("NB_PM_MRM_NON_NULLE"),
        )
        .orderBy(clause_col, "TYPE_CLAUSE", F.desc("PM_CPT_TOTAL"))
    )

