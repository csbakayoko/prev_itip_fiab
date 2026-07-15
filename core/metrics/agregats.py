"""
Métriques par axe — ré-agrégations Spark de df_result.

Chute par clause / ancienneté, consignes par clause (tableau de bord),
anomalies par mois de survenance, investigation des orphelins CPT_ONLY.
Les finalisations (_finalise_*) sont en pure pandas, testables sans Spark.
"""

from typing import Optional

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from config import CODE_GARANTIE_IT, CODE_GARANTIE_IP, MATCH_LABELS, RECUP_NON_LABEL
from core.metrics.base import (
    EXERCICE_INV, EXERCICE_N1, _EXERCICE_ORDRE, _BLOC_ORDRE,
    derive_clause_column, _with_mrm_action, _filter_chute_universe,
    _mois_label_expr, _bloc_anciennete_expr,
)


# ============================================================================
# MÉTRIQUES PAR AXE (ré-agrégation Spark de df_result)
# ============================================================================

def chute_par_clause(df_result: DataFrame, top: Optional[int] = None) -> pd.DataFrame:
    """Taux de chute par clause × exercice, trié par PM MRM — graphe 3.

    Deux blocs EXERCICE : « Inventaire courant » (les stats globales) et
    « Récupérés N+1 » (analyse séparée, hors stats globales). Même univers et
    même formule agrégée que les taux de chute : dans chaque bloc, Σ des
    lignes (Σ écart / Σ PM MRM) redonne le taux correspondant
    (taux_chute_inventaire / taux_chute_n1), et les poids PM se lisent dans
    le bloc. top=N → ne garde que les N clauses de plus forte PM MRM de
    chaque bloc.
    """
    df = (
        _filter_chute_universe(_with_mrm_action(derive_clause_column(df_result)))
        .withColumn("EXERCICE",
            F.when(F.col("TYPE_RECONCILIATION") == "CPT_LATE", F.lit(EXERCICE_N1))
             .otherwise(F.lit(EXERCICE_INV)))
        .withColumn("_ecart", F.coalesce(F.col("MRM_PM"), F.lit(0.0))
                            - F.coalesce(F.col("CPT_PM"), F.lit(0.0)))
    )
    pdf = (
        df.groupBy("EXERCICE", "CLAUSE", "TYPE_COMPTE")
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.sum(F.when(F.col("_ecart") > 0, 1).otherwise(0)).alias("NB_SOUS_PROVISION"),
            F.sum(F.when(F.col("_ecart") < 0, 1).otherwise(0)).alias("NB_SUR_PROVISION"),
            F.sum(F.when(F.col("_ecart") == 0, 1).otherwise(0)).alias("NB_ECART_NUL"),
            F.coalesce(F.sum("MRM_PM"), F.lit(0.0)).alias("PM_MRM"),
            F.coalesce(F.sum("CPT_PM"), F.lit(0.0)).alias("PM_CPT"),
            F.sum("_ecart").alias("ECART"),
        )
        .toPandas()
    )
    return _finalise_chute_par_clause(pdf, top)


def _taux_poids_par_exercice(pdf: pd.DataFrame) -> pd.DataFrame:
    """Taux de chute et poids PM calculés DANS chaque bloc EXERCICE — pure pandas.

    Commun à chute_par_clause et chute_par_anciennete : dans chaque exercice,
    taux = Σécart / ΣPM MRM par ligne ; poids = part de la PM MRM de l'exercice
    (le taux du bloc est la moyenne PONDÉRÉE des taux par ligne, pas leur somme).
    """
    pdf = pdf.copy()
    pdf[["PM_MRM", "PM_CPT", "ECART"]] = pdf[["PM_MRM", "PM_CPT", "ECART"]].round(2)
    pdf["TAUX_CHUTE_PCT"] = (
        (pdf["ECART"] / pdf["PM_MRM"] * 100).where(pdf["PM_MRM"] != 0, 0.0).round(2)
    )
    tot = pdf.groupby("EXERCICE")["PM_MRM"].transform("sum")
    pdf["POIDS_PM_PCT"] = (pdf["PM_MRM"] / tot * 100).where(tot != 0, 0.0).round(2)
    return pdf


def _finalise_chute_par_clause(pdf: pd.DataFrame, top: Optional[int] = None) -> pd.DataFrame:
    """Taux et poids PM par clause × exercice, triés par PM MRM décroissante."""
    pdf = _taux_poids_par_exercice(pdf)
    pdf = (
        pdf.sort_values(["EXERCICE", "PM_MRM"], ascending=[True, False],
                        key=lambda s: s.map(_EXERCICE_ORDRE) if s.name == "EXERCICE" else s)
        .reset_index(drop=True)
    )
    if top:
        pdf = pdf.groupby("EXERCICE", sort=False).head(top).reset_index(drop=True)
    return pdf


def _finalise_chute_par_anciennete(pdf: pd.DataFrame) -> pd.DataFrame:
    """Taux et poids PM par bloc d'ancienneté × exercice, triés N → N-1 → N-2+."""
    pdf = _taux_poids_par_exercice(pdf)
    return (
        pdf.sort_values(
            ["EXERCICE", "BLOC_ANCIENNETE"], ascending=[True, True],
            key=lambda s: s.map(_EXERCICE_ORDRE) if s.name == "EXERCICE"
            else s.map(_BLOC_ORDRE),
        )
        .reset_index(drop=True)
    )


def chute_par_anciennete(
    df_result      : DataFrame,
    annee_inventaire: Optional[int],
    date_col       : str = "CPT_D_SURVENANCE",
) -> pd.DataFrame:
    """Taux de chute par bloc d'ancienneté × exercice — graphe 10.

    Découpe l'univers de chute (même filtre que chute_par_clause) par année de
    survenance relative à l'inventaire : N / N-1 / N-2 et antérieur — la méthode
    d'inventaire diffère selon l'année (revue tête par tête sur N-1). Comme
    chute_par_clause : deux blocs EXERCICE (« Inventaire courant » = stats
    globales / « Récupérés N+1 » = analyse séparée) ; dans chaque bloc, Σ des
    lignes redonne le taux correspondant et les poids PM se lisent par ligne.
    """
    df = (
        _filter_chute_universe(_with_mrm_action(df_result))
        .withColumn("EXERCICE",
            F.when(F.col("TYPE_RECONCILIATION") == "CPT_LATE", F.lit(EXERCICE_N1))
             .otherwise(F.lit(EXERCICE_INV)))
        .withColumn("BLOC_ANCIENNETE", _bloc_anciennete_expr(date_col, annee_inventaire))
        .withColumn("_ecart", F.coalesce(F.col("MRM_PM"), F.lit(0.0))
                            - F.coalesce(F.col("CPT_PM"), F.lit(0.0)))
    )
    pdf = (
        df.groupBy("EXERCICE", "BLOC_ANCIENNETE")
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.sum(F.when(F.col("_ecart") > 0, 1).otherwise(0)).alias("NB_SOUS_PROVISION"),
            F.sum(F.when(F.col("_ecart") < 0, 1).otherwise(0)).alias("NB_SUR_PROVISION"),
            F.sum(F.when(F.col("_ecart") == 0, 1).otherwise(0)).alias("NB_ECART_NUL"),
            F.coalesce(F.sum("MRM_PM"), F.lit(0.0)).alias("PM_MRM"),
            F.coalesce(F.sum("CPT_PM"), F.lit(0.0)).alias("PM_CPT"),
            F.sum("_ecart").alias("ECART"),
        )
        .toPandas()
    )
    return _finalise_chute_par_anciennete(pdf)


def anomalies_cpt_only(
    df_result: DataFrame,
    date_col : str = "CPT_D_SURVENANCE",
    pm_col   : str = "CPT_PM",
) -> pd.DataFrame:
    """Anomalies (CPT sans contrepartie MRM) par mois de survenance — graphe 6.

    Volume et PM compte par mois, avec le marqueur fin d'année
    (Oct-Déc : déclarations tardives probables).
    """
    pdf = (
        df_result
        .filter(F.col("TYPE_RECONCILIATION") == "CPT_ONLY")
        .withColumn("MOIS_SURVENANCE", F.month(F.col(date_col)))
        .withColumn("MOIS_LABEL", _mois_label_expr(date_col))
        .groupBy("MOIS_SURVENANCE", "MOIS_LABEL")
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.round(F.sum(pm_col), 2).alias("PM_CPT"),
        )
        .orderBy("MOIS_SURVENANCE")
        .toPandas()
    )
    if pdf.empty:
        return pd.DataFrame(columns=["MOIS_SURVENANCE", "MOIS_LABEL",
                                     "NB_DOSSIERS", "PM_CPT", "IS_FIN_ANNEE"])
    pdf["IS_FIN_ANNEE"] = pdf["MOIS_SURVENANCE"].isin([10, 11, 12])
    return pdf


# ============================================================================
# CONSIGNES PAR CLAUSE (tableau de bord Power BI)
# ============================================================================

# Ordre d'affichage des consignes (aligné sur l'écran Power BI).
_CONSIGNE_ORDRE = {"KEEP": 0, "ADD": 1, "STUDY": 2, "DELETE": 3}


def _finalise_consignes_par_clause(pdf: pd.DataFrame) -> pd.DataFrame:
    """PCT_SUIVI + libellé CONSIGNE (sans préfixe MRM_) + tri — pure pandas."""
    pdf = pdf.copy()
    pdf["CONSIGNE"] = pdf["MRM_ACTION"].str.replace("MRM_", "", regex=False)
    pdf[["PM_MRM", "PM_CPT"]] = pdf[["PM_MRM", "PM_CPT"]].round(2)
    tot = pdf["NB_SUIVIES"] + pdf["NB_NON_SUIVIES"]
    pdf["PCT_SUIVI"] = (pdf["NB_SUIVIES"] / tot * 100).where(tot != 0, 0.0).round(1)
    pdf = pdf.sort_values(
        ["TYPE_COMPTE", "CLAUSE", "CONSIGNE"],
        key=lambda s: s.map(_CONSIGNE_ORDRE) if s.name == "CONSIGNE" else s,
        na_position="last",
    ).reset_index(drop=True)
    return pdf[["TYPE_COMPTE", "CLAUSE", "CONSIGNE", "NB_DOSSIERS",
                "NB_SUIVIES", "NB_NON_SUIVIES", "PCT_SUIVI",
                "PM_MRM", "PM_CPT", "NB_NON_REMONTE_DF"]]


def consignes_par_clause(df_result: DataFrame) -> pd.DataFrame:
    """Suivi opérationnel des consignes par TYPE_COMPTE × CLAUSE × CONSIGNE.

    Alimente le « tableau de bord des consignes » Power BI : les cartes par
    périmètre (PB / autres) et le tableau filtrable se calculent en DAX par
    simple filtre sur cette table — aucun second run du moteur.

    Exercice courant pur, mêmes règles que la table `consignes` globale :
        KEEP/ADD/STUDY : suivie = dossier retrouvé au compte (matché) ;
                         non suivie = non retrouvé (MRM_MISSING).
        DELETE         : suivie = suppression effective (absent du compte) ;
                         non suivie = encore au compte (matché).
    Les récupérés N+1 (suivi séparé, cf. suivi_n1) sont exclus.

    NB_NON_REMONTE_DF : dossiers repêchés via statut inventaire NON (PM MRM
    = 0, non remontée à la Direction Financière) — hors conformité, comptés
    à part pour la colonne « remonté DF » de l'écran.
    """
    df = derive_clause_column(_with_mrm_action(df_result))

    matched   = F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS))
    missing   = F.col("TYPE_RECONCILIATION") == "MRM_MISSING"
    deleted   = F.col("TYPE_RECONCILIATION") == "MRM_DELETE"
    recup_non = F.col("TYPE_RECONCILIATION") == RECUP_NON_LABEL
    is_delete = F.col("MRM_ACTION") == "MRM_DELETE"

    univers = df.filter(
        F.col("MRM_ACTION").isNotNull() & (matched | missing | deleted | recup_non)
    )
    suivie     = F.when(is_delete, deleted).otherwise(matched)
    non_suivie = F.when(is_delete, matched).otherwise(missing)

    pdf = (
        univers.groupBy("TYPE_COMPTE", "CLAUSE", "MRM_ACTION")
        .agg(
            F.sum(F.when(~recup_non, 1).otherwise(0)).alias("NB_DOSSIERS"),
            F.sum(F.when(~recup_non & suivie, 1).otherwise(0)).alias("NB_SUIVIES"),
            F.sum(F.when(~recup_non & non_suivie, 1).otherwise(0)).alias("NB_NON_SUIVIES"),
            F.coalesce(F.sum(F.when(~recup_non, F.col("MRM_PM"))), F.lit(0.0)).alias("PM_MRM"),
            F.coalesce(F.sum(F.when(~recup_non, F.col("CPT_PM"))), F.lit(0.0)).alias("PM_CPT"),
            F.sum(F.when(recup_non, 1).otherwise(0)).alias("NB_NON_REMONTE_DF"),
        )
        .toPandas()
    )
    if pdf.empty:
        return pd.DataFrame(columns=["TYPE_COMPTE", "CLAUSE", "CONSIGNE",
                                     "NB_DOSSIERS", "NB_SUIVIES", "NB_NON_SUIVIES",
                                     "PCT_SUIVI", "PM_MRM", "PM_CPT",
                                     "NB_NON_REMONTE_DF"])
    return _finalise_consignes_par_clause(pdf)


# ============================================================================
# INVESTIGATION DES ORPHELINS CPT_ONLY
# ============================================================================
# Orphelins du compte préposé : dossiers compte sans contrepartie MRM (anomalies
# définitives, ≡ def_nb / def_pm de la synthèse — les obs tardives et repêchés
# statut NON ont un label distinct, exclus). On les caractérise pour identifier
# le compte PB le plus représentatif (à présenter au souscripteur) et comprendre
# l'orphelinage (clé incomplète, garantie, ancienneté).

_ORPHAN_LABEL = "CPT_ONLY"

# Composantes de la clé de matching à auditer côté compte (suffixes canoniques,
# préfixés CPT_). Les orphelins n'ont pas de colonnes MRM_* → audit côté CPT.
_CLE_COMPONENTS      = ("RPP", "D_NAISSANCE", "D_SURVENANCE", "GARANTIE", "NOM_PRENOM", "CLAUSE")
_CLE_DATE_COMPONENTS = ("D_NAISSANCE", "D_SURVENANCE")


def _orphelins(df: DataFrame) -> DataFrame:
    """Sous-ensemble CPT_ONLY (orphelins compte définitifs)."""
    return df.filter(F.col("TYPE_RECONCILIATION") == F.lit(_ORPHAN_LABEL))


def _finalise_orphelins(pdf: pd.DataFrame, with_rang: bool = False) -> pd.DataFrame:
    """Ajoute les poids (nb, PM) — pure pandas. with_rang : tri nb décroissant +
    RANG (1 = compte/modalité le plus représentatif)."""
    pdf = pdf.copy()
    pdf["PM_CPT"] = pdf["PM_CPT"].round(2)
    tot_nb = pdf["NB_DOSSIERS"].sum() or 1
    tot_pm = pdf["PM_CPT"].sum() or 1.0
    pdf["POIDS_NB_PCT"] = (pdf["NB_DOSSIERS"] / tot_nb * 100).round(2)
    pdf["POIDS_PM_PCT"] = (pdf["PM_CPT"] / tot_pm * 100).round(2)
    if with_rang:
        pdf = pdf.sort_values("NB_DOSSIERS", ascending=False).reset_index(drop=True)
        pdf.insert(0, "RANG", range(1, len(pdf) + 1))
    return pdf


def orphelins_par_clause(df_result: DataFrame) -> pd.DataFrame:
    """Orphelins CPT_ONLY par clause (compte PB) × type — graphe 11.

    RANG 1 = compte PB le plus représentatif : à investiguer avec le souscripteur
    (comment ces listes ont-elles été remontées sans apparaître dans MRM ?).
    """
    pdf = (
        _orphelins(derive_clause_column(df_result))
        .groupBy("CLAUSE", "TYPE_COMPTE")
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.coalesce(F.sum("CPT_PM"), F.lit(0.0)).alias("PM_CPT"),
        )
        .toPandas()
    )
    return _finalise_orphelins(pdf, with_rang=True)[
        ["RANG", "CLAUSE", "TYPE_COMPTE", "NB_DOSSIERS", "PM_CPT",
         "POIDS_NB_PCT", "POIDS_PM_PCT"]
    ]


def orphelins_par_garantie(df_result: DataFrame, garantie_col: str = "CPT_GARANTIE") -> pd.DataFrame:
    """Orphelins CPT_ONLY ventilés par garantie (IT 60 / IP 64 / autre / non renseignée)."""
    g    = F.col(garantie_col)
    code = g.cast("int")
    libelle = (
        F.when(code == F.lit(CODE_GARANTIE_IT), F.lit("IT (incapacité)"))
         .when(code == F.lit(CODE_GARANTIE_IP), F.lit("IP (invalidité)"))
         .when(g.isNull() | (F.trim(g.cast("string")) == F.lit("")), F.lit("Non renseignée"))
         .otherwise(F.concat(F.lit("Autre ("), code.cast("string"), F.lit(")")))
    )
    pdf = (
        _orphelins(df_result)
        .withColumn("GARANTIE_CODE", code)
        .withColumn("GARANTIE_LIBELLE", libelle)
        .groupBy("GARANTIE_CODE", "GARANTIE_LIBELLE")
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.coalesce(F.sum("CPT_PM"), F.lit(0.0)).alias("PM_CPT"),
        )
        .orderBy(F.col("NB_DOSSIERS").desc())
        .toPandas()
    )
    return _finalise_orphelins(pdf)[
        ["GARANTIE_CODE", "GARANTIE_LIBELLE", "NB_DOSSIERS", "PM_CPT",
         "POIDS_NB_PCT", "POIDS_PM_PCT"]
    ]


def orphelins_par_anciennete(
    df_result      : DataFrame,
    annee_inventaire: Optional[int],
    date_col       : str = "CPT_D_SURVENANCE",
) -> pd.DataFrame:
    """Orphelins CPT_ONLY par ancienneté (N / N-1 / N-2 et antérieur)."""
    pdf = (
        _orphelins(df_result)
        .withColumn("BLOC_ANCIENNETE", _bloc_anciennete_expr(date_col, annee_inventaire))
        .groupBy("BLOC_ANCIENNETE")
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.coalesce(F.sum("CPT_PM"), F.lit(0.0)).alias("PM_CPT"),
        )
        .toPandas()
    )
    pdf = _finalise_orphelins(pdf)
    return (
        pdf.sort_values("BLOC_ANCIENNETE", key=lambda s: s.map(_BLOC_ORDRE))
        .reset_index(drop=True)[
            ["BLOC_ANCIENNETE", "NB_DOSSIERS", "PM_CPT", "POIDS_NB_PCT", "POIDS_PM_PCT"]
        ]
    )


def orphelins_cles_nulles(df_result: DataFrame) -> pd.DataFrame:
    """Nullité des colonnes constitutives de la clé pour les orphelins CPT_ONLY.

    Une composante souvent nulle/vide (ex. RPP, clause) explique l'orphelinage :
    la clé concat_ws l'ignore → pas de rapprochement possible. Une ligne par
    composante, triée par fréquence de nullité décroissante.
    """
    orph  = _orphelins(df_result)
    comps = [c for c in _CLE_COMPONENTS if f"CPT_{c}" in orph.columns]
    exprs = [F.count("*").alias("_total")]
    for c in comps:
        col  = F.col(f"CPT_{c}")
        cond = col.isNull() if c in _CLE_DATE_COMPONENTS else (
            col.isNull() | (F.trim(col.cast("string")) == F.lit(""))
        )
        exprs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(c))
    row   = orph.agg(*exprs).first()
    total = int(row["_total"] or 0) if row else 0
    rows  = [{
        "COMPOSANTE"        : f"CPT_{c}",
        "NB_NULL_OU_VIDE"   : int(row[c] or 0) if row else 0,
        "PCT_NULL"          : round((int(row[c] or 0) if row else 0) / total * 100, 2) if total else 0.0,
        "NB_TOTAL_ORPHELINS": total,
    } for c in comps]
    return (
        pd.DataFrame(rows, columns=["COMPOSANTE", "NB_NULL_OU_VIDE", "PCT_NULL",
                                    "NB_TOTAL_ORPHELINS"])
        .sort_values("NB_NULL_OU_VIDE", ascending=False)
        .reset_index(drop=True)
    )
