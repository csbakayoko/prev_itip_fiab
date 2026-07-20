"""
Métriques par axe — ré-agrégations Spark de df_result.

Chute par type de compte / ancienneté, consignes par type de compte (tableau de
bord), anomalies par mois de survenance, investigation des orphelins CPT_ONLY.
Les finalisations (_finalise_*) sont en pure pandas, testables sans Spark.

L'axe d'analyse est TYPE_COMPTE (PB / HPB / …), le périmètre métier. La CLAUSE
n'est pas un axe : elle remplace le RPP dans la clé de matching quand celui-ci
est nul, et tous les types de compte n'en portent pas. Elle ne subsiste que
dans `orphelins_par_clause` (détail d'investigation) et comme composante auditée
dans `orphelins_cles_nulles`.
"""

from typing import Optional

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from config import CODE_GARANTIE_IT, CODE_GARANTIE_IP, MATCH_LABELS, RECUP_NON_LABEL
from core.synthese.synthese_contract import SyntheseScalars
from core.metrics.base import (
    EXERCICE_INV, EXERCICE_N1, _EXERCICE_ORDRE, _BLOC_ORDRE,
    _annee_inventaire,
    derive_clause_column, _with_mrm_action, _filter_chute_universe,
    _mois_label_expr, _bloc_anciennete_expr,
)

# Libellés des AXES des tables regroupées `chute` et `orphelins` — un axe = un
# angle d'analyse, la colonne SEGMENT porte la modalité (PB, N-1, Oct, …).
AXE_ENSEMBLE    = "Ensemble"                  # chute : le taux officiel de l'exercice
AXE_TYPE_COMPTE = "Type de compte"
AXE_ANCIENNETE  = "Ancienneté"
AXE_GARANTIE    = "Garantie"
AXE_MOIS        = "Mois de survenance"
AXE_CLAUSE      = "Clause (détail)"           # sous-ensemble : porteurs de clause
AXE_CLE_NULLE   = "Composante de clé nulle"   # fréquences de nullité, non additif


# ============================================================================
# MÉTRIQUES PAR AXE (ré-agrégation Spark de df_result)
# ============================================================================

def chute_par_type_compte(df_result: DataFrame) -> pd.DataFrame:
    """Taux de chute par type de compte × exercice, trié par PM MRM — graphe 3.

    Axe = TYPE_COMPTE (PB / HPB / …), le périmètre métier. La clause n'est PAS
    un axe d'analyse : c'est un substitut du RPP dans la clé de matching, et
    tous les types de compte n'en portent pas.

    Deux blocs EXERCICE : « Inventaire courant » (les stats globales) et
    « Récupérés N+1 » (analyse séparée, hors stats globales). Même univers et
    même formule agrégée que les taux de chute : dans chaque bloc, Σ des
    lignes (Σ écart / Σ PM MRM) redonne le taux correspondant
    (taux_chute_inventaire / taux_chute_n1), et les poids PM se lisent dans
    le bloc.
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
        df.groupBy("EXERCICE", "TYPE_COMPTE")
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
    return _finalise_chute_par_type_compte(pdf)


def _taux_poids_par_exercice(pdf: pd.DataFrame) -> pd.DataFrame:
    """Taux de chute et poids PM calculés DANS chaque bloc EXERCICE — pure pandas.

    Commun à chute_par_type_compte et chute_par_anciennete : dans chaque exercice,
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


def _finalise_chute_par_type_compte(pdf: pd.DataFrame) -> pd.DataFrame:
    """Taux et poids PM par type de compte × exercice, triés par PM MRM décroissante."""
    pdf = _taux_poids_par_exercice(pdf)
    return (
        pdf.sort_values(["EXERCICE", "PM_MRM"], ascending=[True, False],
                        key=lambda s: s.map(_EXERCICE_ORDRE) if s.name == "EXERCICE" else s)
        .reset_index(drop=True)
    )


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

    Découpe l'univers de chute (même filtre que chute_par_type_compte) par année de
    survenance relative à l'inventaire : N / N-1 / N-2 et antérieur — la méthode
    d'inventaire diffère selon l'année (revue tête par tête sur N-1). Comme
    chute_par_type_compte : deux blocs EXERCICE (« Inventaire courant » = stats
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


# ============================================================================
# TABLE REGROUPÉE « CHUTE » — le taux sous tous ses angles
# ============================================================================

_CHUTE_COLONNES = [
    "EXERCICE", "AXE", "SEGMENT", "TYPE_COMPTE",
    "NB_DOSSIERS", "NB_SOUS_PROVISION", "NB_SUR_PROVISION", "NB_ECART_NUL",
    "PM_MRM", "PM_CPT", "ECART", "TAUX_CHUTE_PCT", "POIDS_PM_PCT",
]


def _ensemble_chute(d: SyntheseScalars) -> pd.DataFrame:
    """Lignes AXE = « Ensemble » : le taux de chute OFFICIEL de chaque exercice
    (scalaires de compute_synthese — la référence que Σ de chaque axe ventilé
    doit redonner). Le détail des signes (sous/sur-provision) est porté par
    les axes ventilés."""
    rows = [
        (EXERCICE_INV, d["metrics_nb"],
         d["metrics_pm_mrm"],  d["metrics_pm_cpt"],  d["taux_chute_inventaire"]),
        (EXERCICE_N1,  d["chute_n1_nb"],
         d["chute_n1_pm_mrm"], d["chute_n1_pm_cpt"], d["taux_chute_n1"]),
    ]
    return pd.DataFrame([{
        "EXERCICE"          : lbl,
        "AXE"               : AXE_ENSEMBLE,
        "SEGMENT"           : AXE_ENSEMBLE,
        "NB_DOSSIERS"       : nb,
        "NB_SOUS_PROVISION" : None,
        "NB_SUR_PROVISION"  : None,
        "NB_ECART_NUL"      : None,
        "PM_MRM"            : pm_mrm,
        "PM_CPT"            : pm_cpt,
        "ECART"             : pm_mrm - pm_cpt,
        "TAUX_CHUTE_PCT"    : taux,
        "POIDS_PM_PCT"      : 100.0,
    } for lbl, nb, pm_mrm, pm_cpt, taux in rows])


def _empiler_axe(pdf: pd.DataFrame, axe: str, segment_col: str) -> pd.DataFrame:
    """Renomme la colonne d'axe en SEGMENT et insère la colonne AXE — pure pandas."""
    pdf = pdf.rename(columns={segment_col: "SEGMENT"}).copy()
    pdf.insert(0, "AXE", axe)
    return pdf


def _assemble_chute(
    ensemble       : pd.DataFrame,
    par_type_compte: pd.DataFrame,
    par_anciennete : pd.DataFrame,
) -> pd.DataFrame:
    """Empile les trois angles dans le schéma commun — pure pandas.

    L'axe « Type de compte » garde sa colonne TYPE_COMPTE en plus de SEGMENT
    (grain de l'axe — cible des relations Power BI), comme dans `orphelins`.
    """
    tc = par_type_compte.copy()
    tc.insert(0, "AXE", AXE_TYPE_COMPTE)
    tc["SEGMENT"] = tc["TYPE_COMPTE"]
    blocs = [
        ensemble,
        tc,
        _empiler_axe(par_anciennete, AXE_ANCIENNETE, "BLOC_ANCIENNETE"),
    ]
    return pd.concat(blocs, ignore_index=True).reindex(columns=_CHUTE_COLONNES)


def chute(df_result: DataFrame, d: SyntheseScalars) -> pd.DataFrame:
    """LE taux de chute sous tous ses angles — une seule table (graphes 3, 7, 10).

    Une ligne par EXERCICE × AXE × SEGMENT :
    - AXE « Ensemble » : le taux officiel de l'exercice (stats globales pour
      l'inventaire courant, analyse séparée pour les récupérés N+1) ;
    - AXE « Type de compte » (PB / HPB / …) et « Ancienneté » (N / N-1 / N-2
      et antérieur) : les ventilations — dans chaque bloc EXERCICE × AXE,
      Σ des lignes (Σ écart / Σ PM MRM) redonne le taux « Ensemble » et les
      poids PM se lisent dans le bloc (garanti par controles_coherence).
    """
    return _assemble_chute(
        _ensemble_chute(d),
        chute_par_type_compte(df_result),
        chute_par_anciennete(df_result, _annee_inventaire(d)),
    )


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
# CONSIGNES PAR TYPE DE COMPTE (tableau de bord Power BI)
# ============================================================================

# Ordre d'affichage des consignes (aligné sur l'écran Power BI).
_CONSIGNE_ORDRE = {"KEEP": 0, "ADD": 1, "STUDY": 2, "DELETE": 3}


def _finalise_consignes_par_type_compte(pdf: pd.DataFrame) -> pd.DataFrame:
    """PCT_SUIVI + libellé CONSIGNE (sans préfixe MRM_) + tri — pure pandas."""
    pdf = pdf.copy()
    pdf["CONSIGNE"] = pdf["MRM_ACTION"].str.replace("MRM_", "", regex=False)
    pdf[["PM_MRM", "PM_CPT"]] = pdf[["PM_MRM", "PM_CPT"]].round(2)
    tot = pdf["NB_SUIVIES"] + pdf["NB_NON_SUIVIES"]
    pdf["PCT_SUIVI"] = (pdf["NB_SUIVIES"] / tot * 100).where(tot != 0, 0.0).round(1)
    pdf = pdf.sort_values(
        ["TYPE_COMPTE", "CONSIGNE"],
        key=lambda s: s.map(_CONSIGNE_ORDRE) if s.name == "CONSIGNE" else s,
        na_position="last",
    ).reset_index(drop=True)
    return pdf[["TYPE_COMPTE", "CONSIGNE", "NB_DOSSIERS",
                "NB_SUIVIES", "NB_NON_SUIVIES", "PCT_SUIVI",
                "PM_MRM", "PM_CPT", "NB_NON_REMONTE_DF"]]


def consignes_par_type_compte(df_result: DataFrame) -> pd.DataFrame:
    """Suivi opérationnel des consignes par TYPE_COMPTE × CONSIGNE.

    Alimente le « tableau de bord des consignes » Power BI : les cartes par
    périmètre (PB / HPB / …) et le tableau filtrable se calculent en DAX par
    simple filtre sur cette table — aucun second run du moteur.

    Exercice courant pur, mêmes règles que la table `consignes` globale :
        KEEP/ADD/STUDY : suivie = dossier retrouvé au compte (matché) ;
                         non suivie = non retrouvé (MRM_MISSING).
        DELETE         : suivie = suppression effective (absent du compte) ;
                         non suivie = encore au compte (matché).
    Les récupérés N+1 (suivi séparé : bloc N+1 de `consignes`) sont exclus.

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
        univers.groupBy("TYPE_COMPTE", "MRM_ACTION")
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
        return pd.DataFrame(columns=["TYPE_COMPTE", "CONSIGNE",
                                     "NB_DOSSIERS", "NB_SUIVIES", "NB_NON_SUIVIES",
                                     "PCT_SUIVI", "PM_MRM", "PM_CPT",
                                     "NB_NON_REMONTE_DF"])
    return _finalise_consignes_par_type_compte(pdf)


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


def _finalise_orphelins(
    pdf      : pd.DataFrame,
    with_rang: bool = False,
    tot_nb   : Optional[int] = None,
    tot_pm   : Optional[float] = None,
) -> pd.DataFrame:
    """Ajoute les poids (nb, PM) — pure pandas.

    with_rang : tri nb décroissant + RANG (1 = modalité la plus représentative).
    tot_nb / tot_pm : dénominateurs des poids. Par défaut les totaux de `pdf`
    (la ventilation partitionne les orphelins) ; à fournir explicitement quand
    la table ne couvre qu'un SOUS-ENSEMBLE des orphelins — les poids restent
    alors lisibles en part du total, pas du sous-ensemble.
    """
    pdf = pdf.copy()
    pdf["PM_CPT"] = pdf["PM_CPT"].round(2)
    tot_nb = (pdf["NB_DOSSIERS"].sum() if tot_nb is None else tot_nb) or 1
    tot_pm = (pdf["PM_CPT"].sum() if tot_pm is None else tot_pm) or 1.0
    pdf["POIDS_NB_PCT"] = (pdf["NB_DOSSIERS"] / tot_nb * 100).round(2)
    pdf["POIDS_PM_PCT"] = (pdf["PM_CPT"] / tot_pm * 100).round(2)
    if with_rang:
        pdf = pdf.sort_values("NB_DOSSIERS", ascending=False).reset_index(drop=True)
        pdf.insert(0, "RANG", range(1, len(pdf) + 1))
    return pdf


def orphelins_par_type_compte(df_result: DataFrame) -> pd.DataFrame:
    """Orphelins CPT_ONLY par type de compte (PB / HPB / …) — graphe 11.

    Ventilation PRINCIPALE des orphelins : elle partitionne tous les CPT_ONLY
    (Σ NB_DOSSIERS = def_nb), quel que soit le type de compte. RANG 1 = le type
    qui en concentre le plus.
    """
    pdf = (
        _orphelins(derive_clause_column(df_result))
        .groupBy("TYPE_COMPTE")
        .agg(
            F.count("*").alias("NB_DOSSIERS"),
            F.coalesce(F.sum("CPT_PM"), F.lit(0.0)).alias("PM_CPT"),
        )
        .toPandas()
    )
    return _finalise_orphelins(pdf, with_rang=True)[
        ["RANG", "TYPE_COMPTE", "NB_DOSSIERS", "PM_CPT",
         "POIDS_NB_PCT", "POIDS_PM_PCT"]
    ]


def orphelins_par_clause(df_result: DataFrame) -> pd.DataFrame:
    """Orphelins CPT_ONLY par clause — table de DÉTAIL, investigation.

    Seules les lignes PORTANT une clause y figurent : la clause n'existe pas sur
    tous les types de compte (elle sert de substitut au RPP dans la clé, côté
    PB). Cette table ne partitionne donc PAS les orphelins — pour un total, voir
    `orphelins_par_type_compte`. Les poids restent calculés sur l'ensemble des
    orphelins, pour se lire en part du total et non du sous-ensemble.

    RANG 1 = le compte le plus représentatif : à investiguer avec le souscripteur
    (comment ces dossiers ont-ils été remontés sans apparaître dans MRM ?).
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
    # Dénominateurs AVANT filtre : tous les orphelins, porteurs de clause ou non.
    tot_nb = int(pdf["NB_DOSSIERS"].sum()) if len(pdf) else 0
    tot_pm = float(pdf["PM_CPT"].sum()) if len(pdf) else 0.0

    clause = pdf["CLAUSE"]
    pdf    = pdf[clause.notna() & (clause.astype(str).str.strip() != "")]
    if pdf.empty:
        return pd.DataFrame(columns=["RANG", "CLAUSE", "TYPE_COMPTE", "NB_DOSSIERS",
                                     "PM_CPT", "POIDS_NB_PCT", "POIDS_PM_PCT"])
    return _finalise_orphelins(pdf, with_rang=True, tot_nb=tot_nb, tot_pm=tot_pm)[
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


# ============================================================================
# TABLE REGROUPÉE « ORPHELINS » — l'investigation sous tous ses angles
# ============================================================================

_ORPHELINS_COLONNES = [
    "AXE", "SEGMENT", "TYPE_COMPTE", "ORDRE",
    "NB_DOSSIERS", "PM_CPT", "POIDS_NB_PCT", "POIDS_PM_PCT",
]


def _assemble_orphelins(
    par_type_compte: pd.DataFrame,
    par_garantie   : pd.DataFrame,
    par_anciennete : pd.DataFrame,
    par_mois       : pd.DataFrame,
    par_clause     : pd.DataFrame,
    cles_nulles    : pd.DataFrame,
) -> pd.DataFrame:
    """Empile les six angles dans le schéma commun — pure pandas.

    ORDRE = ordre de lecture dans l'axe : rang de représentativité quand il
    existe (type de compte, clause), position triée sinon (garantie par
    volume, ancienneté N → N-2+, mois chronologique, composantes par nullité).
    TYPE_COMPTE n'est renseigné que là où il fait partie du grain (type de
    compte, clause).
    """
    tc = par_type_compte.copy()
    tc["AXE"], tc["SEGMENT"], tc["ORDRE"] = AXE_TYPE_COMPTE, tc["TYPE_COMPTE"], tc["RANG"]

    gar = par_garantie.copy()
    gar["AXE"], gar["SEGMENT"], gar["ORDRE"] = AXE_GARANTIE, gar["GARANTIE_LIBELLE"], range(1, len(gar) + 1)

    anc = par_anciennete.copy()
    anc["AXE"], anc["SEGMENT"], anc["ORDRE"] = AXE_ANCIENNETE, anc["BLOC_ANCIENNETE"], range(1, len(anc) + 1)

    mois = _finalise_orphelins(par_mois[["MOIS_SURVENANCE", "MOIS_LABEL", "NB_DOSSIERS", "PM_CPT"]])
    mois["AXE"], mois["SEGMENT"], mois["ORDRE"] = AXE_MOIS, mois["MOIS_LABEL"], mois["MOIS_SURVENANCE"]

    cla = par_clause.copy()
    cla["AXE"], cla["SEGMENT"], cla["ORDRE"] = AXE_CLAUSE, cla["CLAUSE"], cla["RANG"]

    cle = cles_nulles.copy()
    cle["AXE"], cle["SEGMENT"], cle["ORDRE"] = AXE_CLE_NULLE, cle["COMPOSANTE"], range(1, len(cle) + 1)
    cle["NB_DOSSIERS"], cle["POIDS_NB_PCT"] = cle["NB_NULL_OU_VIDE"], cle["PCT_NULL"]

    return (
        pd.concat([tc, gar, anc, mois, cla, cle], ignore_index=True)
        .reindex(columns=_ORPHELINS_COLONNES)
    )


def orphelins(df_result: DataFrame, annee_inventaire: Optional[int]) -> pd.DataFrame:
    """L'investigation des orphelins CPT_ONLY — une seule table, six angles
    (graphes 6 et 11).

    Une ligne par AXE × SEGMENT. Quatre axes PARTITIONNENT les orphelins
    (Σ NB_DOSSIERS = total des orphelins, chacun — garanti par
    controles_coherence) : « Type de compte », « Garantie », « Ancienneté »,
    « Mois de survenance ». Deux axes de détail ne partitionnent pas :
    « Clause (détail) » (seuls les porteurs de clause, Σ ≤ total) et
    « Composante de clé nulle » (fréquence de nullité de chaque composante —
    un même dossier peut compter plusieurs fois). Les poids se lisent
    TOUJOURS en part du total des orphelins.
    """
    return _assemble_orphelins(
        orphelins_par_type_compte(df_result),
        orphelins_par_garantie(df_result),
        orphelins_par_anciennete(df_result, annee_inventaire),
        anomalies_cpt_only(df_result),
        orphelins_par_clause(df_result),
        orphelins_cles_nulles(df_result),
    )
