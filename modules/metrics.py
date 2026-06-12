"""
Couche métriques — calcul des données de restitution depuis df_result.

Sépare le CALCUL des données (ici) de leur RESTITUTION (modules.viz, Excel,
Power BI). Des fonctions simples, appelées depuis main :

    - les métriques scalaires prennent `d`, le dict de compute_synthese
      (UNE passe Spark, déjà faite par print_synthese dans main) et renvoient
      un DataFrame pandas tidy ;
    - deux métriques par-axe (chute_par_clause, anomalies_cpt_only) prennent
      df_result et ré-agrègent côté Spark ;
    - toutes_metriques / export_metriques orchestrent l'ensemble.

Les fonctions renvoient des DONNÉES BRUTES (nombres, pas de chaînes formatées) :
le formatage (M€, %, séparateurs FR) reste au niveau restitution.

Correspondance avec les 9 graphiques (modules.viz) :
    1. compte_justification   → compte_justification(d)
    2. couverture_mrm         → couverture_mrm(d)
    3. chute_par_clause       → chute_par_clause(df_result)  [× exercice]
    4. chute_par_consigne     → chute_par_consigne(d)
    5. conformite_consignes   → conformite_consignes(d)
    6. anomalies_cpt_only     → anomalies_cpt_only(df_result)
    7. kpi_chute              → taux_chute(d)
    8. kpi_conformite_globale → conformite_globale(d)
    9. pm_par_consigne        → pm_par_consigne(d)

Usage (main / notebook) :
    from modules import metrics

    d = print_synthese(df_result)              # la passe Spark
    metrics.bilan_cas(d)                       # le bilan cas par cas
    metrics.consignes(d)                       # DataFrame pandas
    metrics.chute_par_clause(df_result)        # ré-agrégation Spark → pandas
    metrics.export_metriques(df_result, d)     # tout sur DBFS
"""

import os
from typing import Dict, Iterable, Optional

import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from config import (
    CLIENT_NAME, CLIENT_CLAUSES, MATCH_LABELS, TYPE_CLAUSE_CPT_PREFIX,
)
from modules.kpi_export import compute_synthese, kas_totaux
from modules.matching import categorize_mrm_conclusion


# ============================================================================
# CHEMINS D'EXPORT (DBFS)
# ============================================================================

# Libellé de périmètre pour nommer les sorties : la clause si le run est
# filtré sur une seule, sinon "MULTI". La clause réelle reste DANS les tables.
_PERIMETRE = CLIENT_CLAUSES[0] if (CLIENT_CLAUSES and len(CLIENT_CLAUSES) == 1) else "MULTI"

DEFAULT_BASE_PATH = (
    "dbfs:/FileStore/shared_uploads/cheickseko.bakayoko@axa.fr/itip_fiab_exports"
)


def _to_local(path: str) -> str:
    """Convertit un chemin dbfs:/... en /dbfs/... pour les writers locaux (pandas)."""
    return path.replace("dbfs:/", "/dbfs/", 1) if path.startswith("dbfs:/") else path


def output_dir(base_path: str = DEFAULT_BASE_PATH, sub: str = "") -> str:
    """Sous-dossier d'export propre au périmètre (<base>/<CLIENT>_<PERIM>[/sub])."""
    out = f"{base_path.rstrip('/')}/{CLIENT_NAME}_{_PERIMETRE}"
    return f"{out}/{sub}" if sub else out


# ============================================================================
# HELPERS SPARK (clause + univers de chute)
# ============================================================================

# Préfixe CPT → type de clause (ex. "CPB" → "PB"). Réciproque de
# TYPE_CLAUSE_CPT_PREFIX, pour dériver le type des dossiers sans contrepartie MRM.
_CPT_PREFIX_TO_TYPE = {v.rstrip("_"): t for t, v in TYPE_CLAUSE_CPT_PREFIX.items()}


def derive_clause_column(df: DataFrame) -> DataFrame:
    """
    Ajoute les colonnes CLAUSE et TYPE_CLAUSE attendues par les agrégations
    par clause. Après le waterfall la clause est portée par CPT_CLAUSE
    (ex. "CPB_121981", préfixe = type) et/ou MRM_CLAUSE (ex. "121981") :

        CLAUSE      = MRM_CLAUSE sinon CPT_CLAUSE sans son préfixe ("CPB_…").
        TYPE_CLAUSE = MRM_TYPE_CLAUSE sinon type déduit du préfixe CPT
                      (CPT_ONLY : pas de MRM → on lit le type dans "CPB_…").
    """
    clause_parts = []
    if "MRM_CLAUSE" in df.columns:
        clause_parts.append(F.col("MRM_CLAUSE"))
    if "CPT_CLAUSE" in df.columns:
        clause_parts.append(F.regexp_replace(F.col("CPT_CLAUSE"), r"^[A-Za-z]+_", ""))
    clause = F.coalesce(*clause_parts) if clause_parts else F.lit(None).cast("string")

    type_parts = []
    if "MRM_TYPE_CLAUSE" in df.columns:
        type_parts.append(F.col("MRM_TYPE_CLAUSE"))
    if "CPT_CLAUSE" in df.columns:
        prefix = F.regexp_extract(F.col("CPT_CLAUSE"), r"^([A-Za-z]+)_", 1)
        type_from_cpt = F.lit(None).cast("string")
        for pfx, t in _CPT_PREFIX_TO_TYPE.items():
            type_from_cpt = F.when(prefix == pfx, F.lit(t)).otherwise(type_from_cpt)
        type_parts.append(type_from_cpt)
    type_clause = F.coalesce(*type_parts) if type_parts else F.lit(None).cast("string")

    return df.withColumn("CLAUSE", clause).withColumn("TYPE_CLAUSE", type_clause)


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


# Libellés des deux EXERCICES de chute (chute_par_exercice, chute_par_clause) :
# inventaire courant = les stats globales ; N+1 = analyse séparée.
EXERCICE_INV    = "Inventaire courant"
EXERCICE_N1     = "Récupérés N+1"
_EXERCICE_ORDRE = {EXERCICE_INV: 0, EXERCICE_N1: 1}


# ============================================================================
# MÉTRIQUES SCALAIRES (reshape du dict de compute_synthese)
# ============================================================================

def synthese(d: dict) -> pd.DataFrame:
    """Tous les indicateurs de tête en une ligne (historisable par run)."""
    cons = d["consignes"]
    return pd.DataFrame([{
        "DATE_INVENTAIRE"        : d["date_inventaire"],
        "TAUX_CHUTE_PCT"         : d["taux_chute_inventaire"],
        "TAUX_CHUTE_N1_PCT"      : d["taux_chute_n1"],
        "CONFORMITE_GLOBALE_PCT" : d["conformite_globale"],
        "TAUX_COUVERTURE_MRM_PCT"    : d["taux_couverture_mrm"],
        "TAUX_COUVERTURE_COMPTE_PCT" : d["taux_couverture_compte"],
        "TAUX_RECUP_TARDIVE_PCT" : d["taux_recup_tardive"],
        "TAUX_RECUP_GLOBAL_PCT"  : d["taux_recup_global"],
        # Retrouvés = tous les matchés + tous les N+1 (bulle de la synthèse) ;
        # base chute = matchés inventaire courant hors « à supprimer » / NON
        # (N+1 et repêchés statut NON : analyses séparées, hors stats globales).
        "NB_RETROUVES"           : d["trouves_nb"],
        "PM_MRM_RETROUVES"       : d["trouves_pm_mrm"],
        "PM_CPT_RETROUVES"       : d["trouves_pm_cpt"],
        "NB_BASE_CHUTE"          : d["metrics_nb"],
        "PM_MRM_BASE_CHUTE"      : d["metrics_pm_mrm"],
        "PM_CPT_BASE_CHUTE"      : d["metrics_pm_cpt"],
        "ECART_BASE_CHUTE"       : d["metrics_pm_ecart"],
        "PM_MRM_TOTALE"          : d["mrm_pm"],
        "PM_CPT_TOTALE"          : d["cpt_pm"],
        "NB_MATCHES"             : d["match_nb"],
        "NB_RECUP_N1"            : d["late_nb"],
        "NB_CPT_ONLY"            : d["def_nb"],
        "NB_MRM_MISSING"         : d["non_mappes_nb"],
        "NB_NON_RETROUVE"        : (cons["À conserver"]["ko"] + cons["À ajouter"]["ko"]
                                    + cons["À étudier"]["ko"]),
        "NB_ENCORE_AU_COMPTE"    : cons["À supprimer"]["ko"],
        "COHERENT"               : d["coherent"],
    }])


def bilan_cas(d: dict) -> pd.DataFrame:
    """LE bilan de la réconciliation, cas par cas — la table de présentation.

    Une ligne par cas : matchés de l'inventaire courant (par clé, puis total
    et base du taux de chute), retrouvés par tentatives (N+1, statut NON —
    analyses séparées), non retrouvés de part et d'autre (revue / compte) et
    consigne de suppression non suivie. Chaque cas porte sa volumétrie, ses
    PM, son taux de chute quand il a un sens, et son EXPLICATION (vocabulaire
    client : retrouvé / non retrouvé / encore au compte).
    """
    c_del  = d["consignes"]["À supprimer"]
    del_ko = c_del["nb"] - c_del["conf"]
    rows = [
        # volet, cas, nb, pm_mrm, pm_cpt, taux_chute, explication
        ("Retrouvés — inventaire courant", "Clé principale (nom complet + dates)",
         d["principale_nb"], d["principale_pm"], None, None,
         "contrepartie au compte sur la clé nominale complète (exacte ou fenêtre)"),
        ("Retrouvés — inventaire courant", "Clé affinée (nom tronqué 20 car.)",
         d["affinee_nb"], d["affinee_pm_mrm"], None, None,
         "retrouvé après troncature du nom côté compte"),
        ("Retrouvés — inventaire courant", "Récupération (IT→IP, rechutes)",
         d["recup_nb"], d["recup_pm_mrm"], None, None,
         "retrouvé par bascule de garantie ou rapprochement de rechute"),
        ("Retrouvés — inventaire courant", "TOTAL matchés",
         d["match_nb"], d["match_pm_mrm"], d["match_pm_cpt"], None,
         "tous les dossiers de la revue auditée retrouvés au compte"),
        ("Retrouvés — inventaire courant", "└ base du taux de chute",
         d["metrics_nb"], d["metrics_pm_mrm"], d["metrics_pm_cpt"],
         d["taux_chute_inventaire"],
         "matchés hors « à supprimer » / statut NON — les stats globales (§4.2)"),
        ("Retrouvés par tentatives", "Récupérés dans le MRM N+1",
         d["late_nb"], d["late_pm_mrm"], d["late_pm_cpt"], None,
         "orphelin compte retrouvé dans l'inventaire suivant — analyse séparée, "
         "hors stats globales"),
        ("Retrouvés par tentatives", "└ base chute N+1",
         d["chute_n1_nb"], d["chute_n1_pm_mrm"], d["chute_n1_pm_cpt"],
         d["taux_chute_n1"],
         "hors « à supprimer » N+1 — taux de chute et consignes propres (suivi_n1)"),
        ("Retrouvés par tentatives", "Repêchés via statut inventaire NON",
         d["recup_non_nb"], d["recup_non_pm_mrm"], d["recup_non_pm"], None,
         "anomalie résolue sur un MRM statut NON (PM MRM = 0, non remontée) — "
         "hors métriques"),
        ("Non retrouvés — revue MRM", "À conserver non retrouvé",
         d["keep_nb"], d["keep_pm"], None, None,
         "PM attendue au compte mais absente — à instruire"),
        ("Non retrouvés — revue MRM", "À étudier non retrouvé",
         d["study_nb"], d["study_pm"], None, None,
         "absent du compte — informatif (consigne à étudier)"),
        ("Non retrouvés — revue MRM", "À ajouter non retrouvé",
         d["add_nb"], d["add_pm"], None, None,
         "absent du compte — informatif (consigne à ajouter)"),
        ("Non retrouvés — revue MRM", "À supprimer absents (conformes)",
         d["a_supprimer_nb"], d["a_supprimer_pm"], None, None,
         "suppression suivie : le dossier n'est plus au compte"),
        ("Non retrouvés — compte", "Clos avant inventaire N+1",
         d["obs_nb"], None, d["obs_pm"], None,
         "sinistre clos avant l'inventaire suivant — explicable, pas une anomalie"),
        ("Non retrouvés — compte", "Sans contrepartie (anomalies)",
         d["def_nb"], None, d["def_pm"], None,
         "ni matché, ni récupéré, ni explicable — anomalie à instruire"),
        ("Consigne non suivie", "À supprimer encore au compte",
         del_ko, c_del["pm_mrm"], c_del["pm_cpt"], None,
         "devait disparaître mais retrouvée au compte — hors taux de chute, "
         "suivie via le taux de suppression effective"),
    ]
    return pd.DataFrame([{
        "VOLET"          : volet,
        "CAS"            : cas,
        "NB_DOSSIERS"    : nb,
        "PM_MRM"         : pm_mrm,
        "PM_CPT"         : pm_cpt,
        "ECART"          : (pm_mrm - pm_cpt) if (pm_mrm is not None and pm_cpt is not None) else None,
        "TAUX_CHUTE_PCT" : taux,
        "EXPLICATION"    : expl,
    } for volet, cas, nb, pm_mrm, pm_cpt, taux, expl in rows])


def taux_chute(d: dict) -> pd.DataFrame:
    """LE taux de chute, PM MRM et PM Compte (base chute + retrouvés) — graphe 7.

    Base chute = matchés de l'inventaire courant, hors « à supprimer » et
    hors statut inventaire NON (sans-consigne inclus). Les récupérés N+1
    (TAUX_CHUTE_N1_PCT) sont une analyse séparée, hors stats globales.
    Retrouvés = tous les matchés + tous les N+1 (bulle de la synthèse).
    PM totales = grands totaux des deux univers d'entrée (MRM, Compte).
    """
    return pd.DataFrame([{
        "TAUX_CHUTE_PCT"        : d["taux_chute_inventaire"],
        "TAUX_CHUTE_N1_PCT"     : d["taux_chute_n1"],
        "PM_MRM_BASE_CHUTE"     : d["metrics_pm_mrm"],
        "PM_CPT_BASE_CHUTE"     : d["metrics_pm_cpt"],
        "ECART_BASE_CHUTE"      : d["metrics_pm_ecart"],
        "NB_BASE_CHUTE"         : d["metrics_nb"],
        "NB_RECUP_N1"           : d["chute_n1_nb"],
        "NB_HORS_CONSIGNE"      : d["hors_consigne_nb"],
        "NB_RETROUVES"          : d["trouves_nb"],
        "PM_MRM_RETROUVES"      : d["trouves_pm_mrm"],
        "PM_CPT_RETROUVES"      : d["trouves_pm_cpt"],
        "PM_MRM_TOTALE"         : d["mrm_pm"],
        "PM_CPT_TOTALE"         : d["cpt_pm"],
    }])


def chute_par_exercice(d: dict) -> pd.DataFrame:
    """Taux de chute par exercice de matching : inventaire courant (les stats
    globales) et récupérés N+1 (analyse séparée, hors stats globales).

    Une ligne par exercice — univers disjoints, chacun son taux.
    """
    rows = [
        (EXERCICE_INV, d["metrics_nb"],
         d["metrics_pm_mrm"],  d["metrics_pm_cpt"],  d["taux_chute_inventaire"]),
        (EXERCICE_N1,  d["chute_n1_nb"],
         d["chute_n1_pm_mrm"], d["chute_n1_pm_cpt"], d["taux_chute_n1"]),
    ]
    return pd.DataFrame([{
        "EXERCICE"       : lbl,
        "NB_DOSSIERS"    : nb,
        "PM_MRM"         : pm_mrm,
        "PM_CPT"         : pm_cpt,
        "ECART"          : pm_mrm - pm_cpt,
        "TAUX_CHUTE_PCT" : taux,
    } for lbl, nb, pm_mrm, pm_cpt, taux in rows])


def suivi_n1(d: dict) -> pd.DataFrame:
    """Suivi des consignes des récupérés N+1 (analyse séparée) — une ligne par
    consigne N+1. KEEP/ADD/STUDY = conformes (le dossier est retrouvé) ;
    DELETE = encore au compte ; consigne non reconnue à part."""
    rows = [(consigne, nb, "conforme" if consigne != "À supprimer" else "encore au compte")
            for consigne, nb in d["n1_consignes"].items()]
    if d["n1_sans_consigne"]:
        rows.append(("Sans consigne", d["n1_sans_consigne"], "—"))
    return pd.DataFrame([{
        "CONSIGNE"    : consigne,
        "NB_DOSSIERS" : nb,
        "STATUT"      : statut,
    } for consigne, nb, statut in rows])


def consignes(d: dict) -> pd.DataFrame:
    """Analyse complète par consigne : conformité, PM et taux de chute —
    EXERCICE COURANT pur (les récupérés N+1 ont leur suivi séparé, suivi_n1).

    Une ligne par consigne (conserver / étudier / ajouter / supprimer).
    Couvre à elle seule les graphiques 4 (chute), 5 (conformité) et
    9 (PM revue vs compte) — les fonctions dédiées en sont des vues filtrées.
    """
    rows = []
    for consigne, c in d["consignes"].items():
        rows.append({
            "CONSIGNE"        : consigne,
            "NB_TOTAL"        : c["nb"],
            "NB_CONFORMES"    : c["conf"],
            "PCT_CONFORMITE"  : c["pct"],
            "NB_KO"           : c["ko"],
            "NATURE_KO"       : c["ko_label"],     # non retrouvé | encore au compte
            "NB_BASE_CHUTE"   : c["nb_match"],     # matchés inventaire courant
            "PM_MRM"          : c["pm_mrm"],
            "PM_CPT"          : c["pm_cpt"],
            "ECART"           : c["delta"],
            "TAUX_CHUTE_PCT"  : c["taux_chute"],
            "PM_PERTINENTE"   : c["pertinent"],    # False pour « à supprimer »
        })
    return pd.DataFrame(rows)


def compte_justification(d: dict) -> pd.DataFrame:
    """Décomposition du compte : retrouvés, récupérés N+1, anomalies — graphe 1.

    Une ligne par catégorie (nb + PM compte), avec son poids dans le compte.
    """
    cats = [
        ("Retrouvés (inventaire)",    d["match_nb"],     d["match_pm_cpt"]),
        ("Retrouvés via N+1",         d["late_nb"],      d["late_pm"]),
        ("Repêchés (statut MRM non)", d["recup_non_nb"], d["recup_non_pm"]),
        ("Clos avant inventaire N+1", d["obs_nb"],       d["obs_pm"]),
        ("Sans contrepartie (anom.)", d["def_nb"],       d["def_pm"]),
    ]
    tot_nb = sum(c[1] for c in cats) or 1
    tot_pm = sum(c[2] for c in cats) or 1.0
    return pd.DataFrame([{
        "CATEGORIE"   : lbl,
        "NB_DOSSIERS" : nb,
        "PM_CPT"      : pm,
        "PCT_NB"      : round(nb / tot_nb * 100, 1),
        "PCT_PM"      : round(pm / tot_pm * 100, 1),
    } for lbl, nb, pm in cats])


def couverture_mrm(d: dict) -> pd.DataFrame:
    """Part de la revue MRM retrouvée au compte, non retrouvés par consigne — graphe 2.

    Inclut les « à supprimer » retrouvées au compte (consigne non suivie).
    PCT = part de la revue à comparer (base), sauf « à supprimer » (part de
    sa propre consigne).
    """
    base  = d["a_comparer_nb"] or 1
    c_del = d["consignes"]["À supprimer"]
    del_ko = c_del["nb"] - c_del["conf"]
    rows = [
        ("Retrouvés au compte",                  d["match_nb"], round(d["match_nb"] / base * 100, 1), None),
        ("À conserver non retrouvé",             d["keep_nb"],  round(d["keep_nb"]  / base * 100, 1), d["keep_pm"]),
        ("À étudier non retrouvé",               d["study_nb"], round(d["study_nb"] / base * 100, 1), d["study_pm"]),
        ("À ajouter non retrouvé",               d["add_nb"],   round(d["add_nb"]   / base * 100, 1), d["add_pm"]),
        ("« À supprimer » retrouvées au compte", del_ko,        round(del_ko / (c_del["nb"] or 1) * 100, 1), c_del["pm_mrm"]),
    ]
    return pd.DataFrame([{
        "CATEGORIE"   : lbl,
        "NB_DOSSIERS" : nb,
        "PCT"         : pct,
        "PM_MRM"      : pm,
    } for lbl, nb, pct, pm in rows])


def conformite_globale(d: dict) -> pd.DataFrame:
    """Suivi des consignes au global (exercice courant) — graphe 8 : segments
    conforme / non retrouvé (conserver/étudier/ajouter) + suppression
    effective. Les récupérés N+1 ont leur suivi séparé (suivi_n1).

    Une ligne par segment, deux groupes (KAS = conserver/étudier/ajouter,
    DELETE = à supprimer), avec le taux du groupe.
    """
    k = kas_totaux(d)
    cons = d["consignes"]
    nr = (cons["À conserver"]["ko"] + cons["À ajouter"]["ko"]
          + cons["À étudier"]["ko"])                           # non retrouvés
    c_del  = cons["À supprimer"]
    del_ok = c_del["conf"]
    del_ko = c_del["nb"] - del_ok
    rows = [
        ("conserver/étudier/ajouter", "Conforme",        k["conf"], d["conformite_globale"]),
        ("conserver/étudier/ajouter", "Non retrouvé",    nr,        d["conformite_globale"]),
        ("à supprimer",               "Supprimé (OK)",   del_ok,    c_del["pct"]),
        ("à supprimer",               "Encore au compte", del_ko,   c_del["pct"]),
    ]
    return pd.DataFrame([{
        "GROUPE"           : grp,
        "SEGMENT"          : seg,
        "NB_DOSSIERS"      : nb,
        "PCT_CONFORMITE_GROUPE" : pct,
    } for grp, seg, nb, pct in rows])


# ── Vues filtrées de consignes() — graphes 4, 5 et 9 ─────────────────────────

def chute_par_consigne(d: dict) -> pd.DataFrame:
    """Taux de chute par consigne pertinente (= graphe 4)."""
    df = consignes(d)
    return df[df["PM_PERTINENTE"]][
        ["CONSIGNE", "TAUX_CHUTE_PCT", "PM_MRM", "PM_CPT", "ECART"]
    ].reset_index(drop=True)


def pm_par_consigne(d: dict) -> pd.DataFrame:
    """PM revue MRM vs PM compte par consigne pertinente (= graphe 9)."""
    df = consignes(d)
    return df[df["PM_PERTINENTE"]][
        ["CONSIGNE", "PM_MRM", "PM_CPT", "ECART", "TAUX_CHUTE_PCT"]
    ].reset_index(drop=True)


def conformite_consignes(d: dict) -> pd.DataFrame:
    """Conformité par consigne, toutes consignes (= graphe 5)."""
    out = consignes(d)[
        ["CONSIGNE", "NB_TOTAL", "NB_CONFORMES", "PCT_CONFORMITE", "NB_KO", "NATURE_KO"]
    ].copy()
    out["PCT_KO"] = (100 - out["PCT_CONFORMITE"]).round(1)
    return out


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
        df.groupBy("EXERCICE", "CLAUSE", "TYPE_CLAUSE")
        .agg(
            F.count("*").alias("nb_dossiers"),
            F.sum(F.when(F.col("_ecart") > 0, 1).otherwise(0)).alias("nb_sous"),
            F.sum(F.when(F.col("_ecart") < 0, 1).otherwise(0)).alias("nb_sur"),
            F.sum(F.when(F.col("_ecart") == 0, 1).otherwise(0)).alias("nb_conforme"),
            F.coalesce(F.sum("MRM_PM"), F.lit(0.0)).alias("pm_mrm"),
            F.coalesce(F.sum("CPT_PM"), F.lit(0.0)).alias("pm_cpt"),
            F.sum("_ecart").alias("ecart_signe"),
        )
        .toPandas()
    )
    return _finalise_chute_par_clause(pdf, top)


def _finalise_chute_par_clause(pdf: pd.DataFrame, top: Optional[int] = None) -> pd.DataFrame:
    """Taux et poids PM calculés DANS chaque bloc EXERCICE — pure pandas
    (vérifiable sans Spark)."""
    pdf = pdf.copy()
    pdf[["pm_mrm", "pm_cpt", "ecart_signe"]] = pdf[["pm_mrm", "pm_cpt", "ecart_signe"]].round(2)
    pdf["taux_chute_pct"] = (
        (pdf["ecart_signe"] / pdf["pm_mrm"] * 100).where(pdf["pm_mrm"] != 0, 0.0).round(2)
    )
    # Poids de la clause dans la PM MRM de SON exercice : le taux du bloc est
    # la moyenne PONDÉRÉE des taux par clause (pas leur somme).
    tot = pdf.groupby("EXERCICE")["pm_mrm"].transform("sum")
    pdf["poids_pm_pct"] = (pdf["pm_mrm"] / tot * 100).where(tot != 0, 0.0).round(2)
    pdf = (
        pdf.sort_values(["EXERCICE", "pm_mrm"], ascending=[True, False],
                        key=lambda s: s.map(_EXERCICE_ORDRE) if s.name == "EXERCICE" else s)
        .reset_index(drop=True)
    )
    if top:
        pdf = pdf.groupby("EXERCICE", sort=False).head(top).reset_index(drop=True)
    return pdf


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
# CONTRÔLES DE COHÉRENCE INTER-TABLES
# ============================================================================

def controles_coherence(tables: Dict[str, pd.DataFrame], d: dict) -> pd.DataFrame:
    """Recoupements INTER-TABLES : une même grandeur doit avoir la même valeur
    dans tous les onglets Power BI — l'étude raconte UNE histoire.

    Les tables issues de `d` se recoupent par construction ; les contrôles
    portent surtout sur les ré-agrégations Spark (chute_par_clause,
    anomalies_cpt_only) et les sommes internes (consignes, bilan_cas,
    couverture, suivi N+1). Une ligne par contrôle : attendu, obtenu, OK.
    Exportée avec les autres tables ; bloquante dans le run de production.
    """
    rows = []

    def ctrl(nom, attendu, obtenu, tol=0.0):
        ecart = (obtenu or 0) - (attendu or 0)
        rows.append({"CONTROLE": nom, "ATTENDU": attendu, "OBTENU": obtenu,
                     "ECART": round(ecart, 2), "OK": abs(ecart) <= tol})

    # chute_par_clause (ré-agrégation Spark) vs base chute / bloc N+1 de d.
    # Tolérance 1 € sur les PM : arrondi à 2 décimales par clause.
    cl  = tables["chute_par_clause"]
    inv = cl[cl["EXERCICE"] == EXERCICE_INV]
    n1  = cl[cl["EXERCICE"] == EXERCICE_N1]
    ctrl("chute_par_clause inv. : Σ nb = base chute",     d["metrics_nb"],     int(inv["nb_dossiers"].sum()))
    ctrl("chute_par_clause inv. : Σ PM MRM = base chute", d["metrics_pm_mrm"], float(inv["pm_mrm"].sum()), tol=1.0)
    ctrl("chute_par_clause inv. : Σ PM CPT = base chute", d["metrics_pm_cpt"], float(inv["pm_cpt"].sum()), tol=1.0)
    ctrl("chute_par_clause N+1 : Σ nb = base chute N+1",  d["chute_n1_nb"],    int(n1["nb_dossiers"].sum()))
    ctrl("chute_par_clause N+1 : Σ PM MRM = base N+1",    d["chute_n1_pm_mrm"], float(n1["pm_mrm"].sum()), tol=1.0)

    # anomalies_cpt_only (ré-agrégation Spark) vs CPT_ONLY de d.
    anom = tables["anomalies_cpt_only"]
    ctrl("anomalies : Σ nb = CPT_ONLY",     d["def_nb"], int(anom["NB_DOSSIERS"].sum()))
    ctrl("anomalies : Σ PM CPT = CPT_ONLY", d["def_pm"], float(anom["PM_CPT"].sum()), tol=1.0)

    # consignes : Σ bases KAS + hors consigne == base chute (cf. chute_coherente).
    kas = tables["consignes"][tables["consignes"]["PM_PERTINENTE"]]
    ctrl("consignes : Σ base KAS + hors consigne = base chute",
         d["metrics_nb"], int(kas["NB_BASE_CHUTE"].sum()) + d["hors_consigne_nb"])
    ctrl("consignes : Σ PM MRM KAS + hors consigne = base chute",
         d["metrics_pm_mrm"], float(kas["PM_MRM"].sum()) + d["hors_consigne_pm_mrm"], tol=1.0)

    # bilan_cas : totaux internes + recoupement avec la synthèse.
    b = tables["bilan_cas"].set_index("CAS")
    ctrl("bilan_cas : TOTAL matchés = Σ des 3 clés",
         int(b.loc["TOTAL matchés", "NB_DOSSIERS"]),
         int(b.loc["Clé principale (nom complet + dates)", "NB_DOSSIERS"]
             + b.loc["Clé affinée (nom tronqué 20 car.)", "NB_DOSSIERS"]
             + b.loc["Récupération (IT→IP, rechutes)", "NB_DOSSIERS"]))
    ctrl("bilan_cas : base chute = synthese",
         int(tables["synthese"]["NB_BASE_CHUTE"].iloc[0]),
         int(b.loc["└ base du taux de chute", "NB_DOSSIERS"]))

    # suivi_n1 : conformes + sans consigne == base chute N+1 (DELETE exclu).
    sn1 = tables["suivi_n1"]
    ctrl("suivi_n1 : conformes + sans consigne = base chute N+1",
         d["chute_n1_nb"],
         int(sn1.loc[sn1["STATUT"] != "encore au compte", "NB_DOSSIERS"].sum()))

    # compte_justification : Σ catégories == compte entier.
    ctrl("compte_justification : Σ nb = compte",
         d["cpt_nb"], int(tables["compte_justification"]["NB_DOSSIERS"].sum()))

    # couverture_mrm : retrouvés + non retrouvés == revue à comparer.
    cm = tables["couverture_mrm"]
    ctrl("couverture_mrm : retrouvés + non retrouvés = à comparer",
         d["a_comparer_nb"],
         int(cm.loc[~cm["CATEGORIE"].str.contains("supprimer"), "NB_DOSSIERS"].sum()))

    # chute_par_exercice == taux_chute (mêmes scalaires, deux onglets).
    ce = tables["chute_par_exercice"]
    ctrl("chute_par_exercice = taux_chute (base chute)",
         int(tables["taux_chute"]["NB_BASE_CHUTE"].iloc[0]),
         int(ce.loc[ce["EXERCICE"] == EXERCICE_INV, "NB_DOSSIERS"].iloc[0]))

    return pd.DataFrame(rows)


# ============================================================================
# ORCHESTRATION
# ============================================================================

def toutes_metriques(df_result: DataFrame, d: Optional[dict] = None) -> Dict[str, pd.DataFrame]:
    """Toutes les métriques en un dict {nom: DataFrame pandas}.

    `d` = dict de compute_synthese si déjà calculé (ex. retour de
    print_synthese) — sinon la passe Spark est lancée ici.
    """
    d = d if d is not None else compute_synthese(df_result)
    tables = {
        "synthese"             : synthese(d),
        "bilan_cas"            : bilan_cas(d),
        "taux_chute"           : taux_chute(d),
        "chute_par_exercice"   : chute_par_exercice(d),
        "suivi_n1"             : suivi_n1(d),
        "consignes"            : consignes(d),
        "compte_justification" : compte_justification(d),
        "couverture_mrm"       : couverture_mrm(d),
        "chute_par_clause"     : chute_par_clause(df_result),
        "chute_par_consigne"   : chute_par_consigne(d),
        "conformite_consignes" : conformite_consignes(d),
        "anomalies_cpt_only"   : anomalies_cpt_only(df_result),
        "conformite_globale"   : conformite_globale(d),
        "pm_par_consigne"      : pm_par_consigne(d),
    }
    # Les onglets Power BI doivent se recouper : contrôles inter-tables,
    # exportés avec le reste (bloquants dans le run de production).
    tables["controles_coherence"] = controles_coherence(tables, d)
    return tables


def export_metriques(
    df_result   : DataFrame,
    d           : Optional[dict] = None,
    base_path   : str = DEFAULT_BASE_PATH,
    formats     : Iterable[str] = ("csv", "json", "parquet"),
    delta_schema: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Écrit toutes les métriques sur DBFS, sous <base>/<CLIENT>_<PERIM>/metrics.

    formats ⊆ {csv, json, parquet, excel, delta} — excel produit un seul
    .xlsx multi-onglets ; delta requiert delta_schema (une table
    <schema>.itip_metric_<nom>_<perim> par métrique).
    """
    tables  = toutes_metriques(df_result, d)
    formats = {f.lower() for f in formats}
    out = _to_local(output_dir(base_path, "metrics"))
    os.makedirs(out, exist_ok=True)
    print(f"[METRICS] périmètre {CLIENT_NAME} / clauses {_PERIMETRE} → {sorted(formats)}")

    ctrl = tables["controles_coherence"]
    ko = ctrl[~ctrl["OK"]]
    if len(ko):
        print(f"[METRICS] ✘ {len(ko)} contrôle(s) inter-tables KO — les onglets Power BI ne se recoupent pas :")
        for _, r in ko.iterrows():
            print(f"    ✘ {r['CONTROLE']} : attendu {r['ATTENDU']}, obtenu {r['OBTENU']}")
    else:
        print(f"[METRICS] ✔ contrôles inter-tables : {len(ctrl)}/{len(ctrl)} OK")

    for name, pdf in tables.items():
        if "csv" in formats:
            pdf.to_csv(f"{out}/{name}.csv", index=False, sep=";", encoding="utf-8")
            print(f"  ✓ [CSV]     {out}/{name}.csv")
        if "json" in formats:
            pdf.to_json(f"{out}/{name}.json", orient="records",
                        force_ascii=False, indent=2, date_format="iso")
            print(f"  ✓ [JSON]    {out}/{name}.json")
        if "parquet" in formats:
            pdf.to_parquet(f"{out}/{name}.parquet", index=False)
            print(f"  ✓ [PARQUET] {out}/{name}.parquet")
        if "delta" in formats and delta_schema:
            table = f"{delta_schema}.itip_metric_{name}_{_PERIMETRE}"
            (df_result.sparkSession.createDataFrame(pdf)
                      .write.format("delta").mode("overwrite")
                      .option("overwriteSchema", "true").saveAsTable(table))
            print(f"  ✓ [DELTA]   {table}")

    if "excel" in formats:
        path = f"{out}/metrics_{CLIENT_NAME}_{_PERIMETRE}.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for name, pdf in tables.items():
                pdf.to_excel(writer, sheet_name=name[:31], index=False)
        print(f"  ✓ [EXCEL]   {path}")

    print(f"[METRICS] export terminé → {out}\n")
    return tables
