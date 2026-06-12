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
    3. chute_par_clause       → chute_par_clause(df_result)
    4. chute_par_consigne     → chute_par_consigne(d)
    5. conformite_consignes   → conformite_consignes(d)
    6. anomalies_cpt_only     → anomalies_cpt_only(df_result)
    7. kpi_chute_globale      → taux_chute_global(d)
    8. kpi_conformite_globale → conformite_globale(d)
    9. pm_par_consigne        → pm_par_consigne(d)

Usage (main / notebook) :
    from modules import metrics

    d = print_synthese(df_result)              # la passe Spark
    metrics.consignes(d)                       # DataFrame pandas
    metrics.chute_par_clause(df_result)        # ré-agrégation Spark → pandas
    metrics.export_metriques(df_result, d)     # tout sur DBFS
"""

import os
from typing import Dict, Iterable, Optional

import pandas as pd
from pyspark.sql import DataFrame, Window
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
    """Univers UNIQUE du taux de chute : matchés de l'inventaire courant HORS
    « à supprimer » (les DELETE retrouvées au compte sont analysées à part) +
    TOUS les récupérés N+1, hors statut inventaire NON. Les sans-consigne
    reconnue (MRM_ACTION null) restent inclus. Garantit la cohérence du taux
    par clause ↔ par consigne ↔ global (cf. docs/METRIQUES.md §4).
    CPT_OBS_TARDIVE / CPT_RECUP_NON exclus (jamais matchés / PM MRM = 0)."""
    cond = (
        F.col("TYPE_RECONCILIATION") == "CPT_LATE"
    ) | (
        F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS))
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


# ============================================================================
# MÉTRIQUES SCALAIRES (reshape du dict de compute_synthese)
# ============================================================================

def synthese(d: dict) -> pd.DataFrame:
    """Tous les indicateurs de tête en une ligne (historisable par run)."""
    cons = d["consignes"]
    return pd.DataFrame([{
        "DATE_INVENTAIRE"        : d["date_inventaire"],
        "TAUX_CHUTE_GLOBAL_PCT"  : d["taux_chute_global"],
        "TAUX_CHUTE_INVENTAIRE_PCT" : d["taux_chute_inventaire"],
        "TAUX_CHUTE_N1_PCT"      : d["taux_chute_n1"],
        "CONFORMITE_GLOBALE_PCT" : d["conformite_globale"],
        "TAUX_COUVERTURE_MRM_PCT"    : d["taux_couverture_mrm"],
        "TAUX_COUVERTURE_COMPTE_PCT" : d["taux_couverture_compte"],
        "TAUX_RECUP_TARDIVE_PCT" : d["taux_recup_tardive"],
        "TAUX_RECUP_GLOBAL_PCT"  : d["taux_recup_global"],
        # Retrouvés = tous les matchés + tous les N+1 (bulle de la synthèse) ;
        # base chute = retrouvés hors « à supprimer » au compte.
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


def taux_chute_global(d: dict) -> pd.DataFrame:
    """Taux de chute global, PM MRM et PM Compte (base chute + retrouvés) — graphe 7.

    Base chute GLOBALE = matchés inventaire courant + récupérés N+1, hors
    « à supprimer » et hors statut inventaire NON (sans-consigne inclus) —
    réunion des deux sous-univers détaillés dans chute_par_exercice().
    Retrouvés = tous les matchés + tous les N+1 (bulle de la synthèse).
    PM totales = grands totaux des deux univers d'entrée (MRM, Compte).
    """
    return pd.DataFrame([{
        "TAUX_CHUTE_GLOBAL_PCT" : d["taux_chute_global"],
        "TAUX_CHUTE_INVENTAIRE_PCT" : d["taux_chute_inventaire"],
        "TAUX_CHUTE_N1_PCT"     : d["taux_chute_n1"],
        "PM_MRM_BASE_CHUTE"     : d["metrics_pm_mrm"],
        "PM_CPT_BASE_CHUTE"     : d["metrics_pm_cpt"],
        "ECART_BASE_CHUTE"      : d["metrics_pm_ecart"],
        "NB_BASE_CHUTE"         : d["metrics_nb"],
        "NB_INVENTAIRE"         : d["metrics_match_nb"],
        "NB_RECUP_N1"           : d["metrics_late_nb"],
        "NB_HORS_CONSIGNE"      : d["hors_consigne_nb"],
        "NB_RETROUVES"          : d["trouves_nb"],
        "PM_MRM_RETROUVES"      : d["trouves_pm_mrm"],
        "PM_CPT_RETROUVES"      : d["trouves_pm_cpt"],
        "PM_MRM_TOTALE"         : d["mrm_pm"],
        "PM_CPT_TOTALE"         : d["cpt_pm"],
    }])


def chute_par_exercice(d: dict) -> pd.DataFrame:
    """Taux de chute par exercice de matching : inventaire courant, récupérés
    N+1 (analyse séparée) et global (réunion des deux sous-univers disjoints).

    Une ligne par exercice — Σ des composantes inventaire + N+1 = global.
    """
    rows = [
        ("Inventaire courant", d["metrics_match_nb"],
         d["chute_inv_pm_mrm"], d["chute_inv_pm_cpt"], d["taux_chute_inventaire"]),
        ("Récupérés N+1",      d["metrics_late_nb"],
         d["chute_n1_pm_mrm"],  d["chute_n1_pm_cpt"],  d["taux_chute_n1"]),
        ("Global (inv. + N+1)", d["metrics_nb"],
         d["metrics_pm_mrm"],   d["metrics_pm_cpt"],   d["taux_chute_global"]),
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
    """Analyse complète par consigne : conformité, PM et taux de chute.

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
            "NB_BASE_CHUTE"   : c["nb_match"],
            "NB_INVENTAIRE"   : c["nb_inv"],
            "NB_RECUP_N1"     : c["nb_late"],
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
    """Suivi des consignes au global — graphe 8 : segments conforme /
    non retrouvé (consignes conserver/étudier/ajouter) + suppression effective.

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
    """Taux de chute par clause (KEEP/ADD/STUDY confondues), trié par PM MRM — graphe 3.

    Même univers et même formule agrégée que le taux de chute global :
    Σ des lignes (Σ écart / Σ PM MRM) redonne le taux de chute global.
    top=N → ne garde que les N clauses de plus forte PM MRM.
    """
    df = (
        _filter_chute_universe(_with_mrm_action(derive_clause_column(df_result)))
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
        # Poids de la clause dans la PM MRM totale : le global est la moyenne
        # PONDÉRÉE des taux par clause (pas leur somme).
        .withColumn("poids_pm_pct",
            F.round(F.col("pm_mrm") / F.sum("pm_mrm").over(Window.partitionBy()) * 100, 2))
    )
    pdf = (
        agg.toPandas()
        .sort_values("pm_mrm", ascending=False)
        .reset_index(drop=True)
    )
    return pdf.head(top) if top else pdf


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
# ORCHESTRATION
# ============================================================================

def toutes_metriques(df_result: DataFrame, d: Optional[dict] = None) -> Dict[str, pd.DataFrame]:
    """Toutes les métriques en un dict {nom: DataFrame pandas}.

    `d` = dict de compute_synthese si déjà calculé (ex. retour de
    print_synthese) — sinon la passe Spark est lancée ici.
    """
    d = d if d is not None else compute_synthese(df_result)
    return {
        "synthese"             : synthese(d),
        "taux_chute_global"    : taux_chute_global(d),
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
