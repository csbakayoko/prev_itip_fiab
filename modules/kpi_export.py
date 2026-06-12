"""
Synthèse client — vue compacte type "schéma SODEXO".

Produit à chaque run :
  1. Une vue d'ensemble en 3 bulles (MRM / MATCHÉS / COMPTE) avec, pour chaque
     sous-catégorie, la volumétrie en nombre de dossiers et en PM (€).
  2. Un bloc d'INDICATEURS (taux de couverture, récupération tardive, taux de
     chute global, niveaux de PM).
  3. Un bloc SUIVI DES CONSIGNES (taux de conformité par consigne, avec la
     volumétrie des dossiers à PM non nulle).

Décodage des grandeurs (depuis df_result + TYPE_RECONCILIATION) :

    MRM      = MATCHÉS + à supprimer + non mappés (total dossiers MRM en entrée)
    MATCHÉS  = clé principale (EXACT + WINDOW) + clé affinée (TRONC + TRONC_WINDOW)
               + récupération (IP / rechute / rechute tronquée)
    COMPTE   = MATCHÉS + récupérés N+1 (CPT_LATE) + obs tardives IT (anomalie,
               CPT_OBS_TARDIVE) + CPT_ONLY définitifs

    Univers MÉTRIQUES (taux de chute, niveaux de PM) = MATCHÉS + récupérés N+1,
    hors consigne « à supprimer » et hors statut inventaire NON. Les matchés
    sans consigne reconnue (MRM_ACTION null) sont INCLUS dans la chute.
    Les obs tardives IT n'ont jamais matché → EXCLUES des métriques et des taux.

    PM : côté MRM (MRM_PM) pour les ventilations MRM, côté CPT (CPT_PM) pour les CPT.
"""

import logging

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from config import (
    CLIENT_NAME,
    DATE_INVENTAIRE,
    MATCH_LABELS,
    MATCH_PRINCIPALE,
    MATCH_AFFINEE,
    MATCH_RECUPERATION,
    OBS_TARDIVE_LABEL,
    RECUP_NON_LABEL,
)
from modules.matching import categorize_mrm_conclusion
from modules._timing import timed_fn

logger = logging.getLogger(__name__)


_KAS = ("MRM_KEEP", "MRM_ADD", "MRM_STUDY")   # consignes "à comparer" (hors DELETE)


def _pct(num, den) -> float:
    """Pourcentage arrondi à 0.1, 0.0 si dénominateur nul."""
    return round(num / den * 100, 1) if den else 0.0


# ============================================================================
# CALCUL DES SCALAIRES (une seule passe Spark)
# ============================================================================

def compute_synthese(df_result: DataFrame) -> dict:
    """
    Agrège df_result en une passe (nb + PM MRM + PM CPT + volumétrie PM≠0 par
    catégorie) et retourne les scalaires de la synthèse.

    Colonnes attendues : TYPE_RECONCILIATION, MRM_PM, CPT_PM, MRM_CONCLUSION.
    """
    df = df_result.withColumn(
        "MRM_ACTION", categorize_mrm_conclusion(F.col("MRM_CONCLUSION"))
    )
    # LATE_SOURCE absent si aucune récupération tardive n'a tourné → colonne neutre.
    if "LATE_SOURCE" not in df.columns:
        df = df.withColumn("LATE_SOURCE", F.lit(None).cast("string"))
    # Statut inventaire NON : exclu de l'univers de chute. Structurellement les
    # matchés viennent du MRM OUI (split en amont) — la dimension rend la règle
    # explicite et robuste si un MRM non scindé est passé en entrée.
    df = df.withColumn(
        "IS_STATUT_NON",
        F.coalesce(F.upper(F.trim(F.col("MRM_STATUT_INV"))) == "NON", F.lit(False))
        if "MRM_STATUT_INV" in df.columns else F.lit(False),
    )

    rows = (
        df.groupBy("TYPE_RECONCILIATION", "MRM_ACTION", "LATE_SOURCE", "IS_STATUT_NON")
        .agg(
            F.count("*").alias("nb"),
            F.coalesce(F.sum("MRM_PM"), F.lit(0.0)).alias("pm_mrm"),
            F.coalesce(F.sum("CPT_PM"), F.lit(0.0)).alias("pm_cpt"),
            # Volumétrie des dossiers dont la PM est non nulle (non-null ET ≠ 0)
            F.sum(F.when(F.col("MRM_PM").isNotNull() & (F.col("MRM_PM") != 0), 1).otherwise(0)).alias("nb_pm_mrm_nz"),
            F.sum(F.when(F.col("CPT_PM").isNotNull() & (F.col("CPT_PM") != 0), 1).otherwise(0)).alias("nb_pm_cpt_nz"),
        )
        .collect()
    )

    princ    = set(MATCH_PRINCIPALE)
    aff      = set(MATCH_AFFINEE)
    recup    = set(MATCH_RECUPERATION)
    match    = set(MATCH_LABELS)       # matchs LÉGITIMES de l'inventaire courant
    T = lambda r: r["TYPE_RECONCILIATION"]
    A = lambda r: r["MRM_ACTION"]
    S = lambda r: r["LATE_SOURCE"]
    N = lambda r: r["IS_STATUT_NON"]

    def agg(field, pred):
        return sum(r[field] for r in rows if pred(r))

    def mrm(pred):   # → (nb, pm_mrm)
        return agg("nb", pred), agg("pm_mrm", pred)

    def cpt(pred):   # → (nb, pm_cpt)
        return agg("nb", pred), agg("pm_cpt", pred)

    nb_princ, pm_princ      = mrm(lambda r: T(r) in princ)
    nb_aff,   pm_aff_mrm    = mrm(lambda r: T(r) in aff)
    nb_recup, pm_recup_mrm  = mrm(lambda r: T(r) in recup)
    nb_del,   pm_del        = mrm(lambda r: T(r) == "MRM_DELETE")
    nb_miss,  pm_miss       = mrm(lambda r: T(r) == "MRM_MISSING")
    nb_def,   pm_def        = cpt(lambda r: T(r) == "CPT_ONLY")
    # CPT_LATE = uniquement les dossiers RÉELLEMENT retrouvés dans un inventaire
    # ultérieur (N+1). Les observations tardives IT portent désormais un label
    # distinct (OBS_TARDIVE_LABEL) → sorties des CPT_LATE et de l'univers métriques.
    nb_late,  pm_late_cpt   = cpt(lambda r: T(r) == "CPT_LATE")
    pm_late_mrm             = agg("pm_mrm", lambda r: T(r) == "CPT_LATE")

    # Observations tardives IT : ANOMALIES (jamais matchées, sans contrepartie MRM).
    # Présentées à part, exclues des taux et des calculs PM/chute.
    nb_obs,   pm_obs_cpt    = cpt(lambda r: T(r) == OBS_TARDIVE_LABEL)

    # CPT récupérés via un MRM statut NON : anomalie résolue (contrepartie MRM
    # existe, statut NON → PM MRM=0). EXCLUS de toutes les métriques de valeur ;
    # présentés à part dans le compte (analyse dédiée recup_statut_non).
    nb_recup_non, pm_recup_non_cpt = cpt(lambda r: T(r) == RECUP_NON_LABEL)

    # AUTO-CONTRÔLE : l'hypothèse métier « statut NON ⇒ PM MRM = 0 » doit tenir
    # sur les repêchés. Si une PM MRM non nulle apparaît, l'exclusion des
    # métriques n'est plus neutre en valeur → on le signale.
    pm_recup_non_mrm    = agg("pm_mrm",       lambda r: T(r) == RECUP_NON_LABEL)
    nb_recup_non_pm_nz  = agg("nb_pm_mrm_nz", lambda r: T(r) == RECUP_NON_LABEL)
    recup_non_pm_mrm_ok = nb_recup_non_pm_nz == 0 and abs(pm_recup_non_mrm) <= 0.01
    if not recup_non_pm_mrm_ok:
        logger.warning(
            "RECUP_NON : %d dossier(s) repêché(s) via statut NON avec PM MRM "
            "non nulle (total %.2f €) — hypothèse « NON ⇒ PM MRM = 0 » violée, "
            "vérifier la source MRM.",
            nb_recup_non_pm_nz, pm_recup_non_mrm,
        )

    # Matchés légitimes de l'inventaire courant.
    nb_match     = nb_princ + nb_aff + nb_recup
    pm_match_mrm = agg("pm_mrm", lambda r: T(r) in match)
    pm_match_cpt = agg("pm_cpt", lambda r: T(r) in match)

    # Univers MÉTRIQUES PM = matchés légitimes + récupérés N+1 réels (CPT_LATE).
    # Les obs tardives IT (CPT_OBS_TARDIVE) sont EXCLUES : jamais matchées.
    in_metrics = lambda r: T(r) in match or T(r) == "CPT_LATE"

    # Univers CHUTE = tous les matchés (inventaire + N+1), hors consigne
    # « à supprimer » et hors statut inventaire NON. Les matchés sans consigne
    # reconnue (MRM_ACTION null) sont INCLUS — A(r) null ≠ "MRM_DELETE".
    in_chute = lambda r: in_metrics(r) and A(r) != "MRM_DELETE" and not N(r)

    # Totaux exhaustifs des deux univers d'entrée.
    #   MRM en entrée   = matchés + à supprimer + non mappés (CPT_LATE exclu : il
    #                     provient d'un autre inventaire ou n'a pas de contrepartie MRM).
    #   COMPTE en entrée = matchés + récupérés N+1 + récupérés via NON + obs
    #                      tardives + CPT_ONLY définitifs.
    mrm_pm_total = pm_match_mrm + pm_del + pm_miss
    cpt_pm_total = pm_match_cpt + pm_def + pm_late_cpt + pm_obs_cpt + pm_recup_non_cpt

    # ── Sous-ventilation de "Non mappés" : MISSING ∩ consigne ────────────────
    def miss(action):
        return mrm(lambda r: T(r) == "MRM_MISSING" and A(r) == action)
    keep_miss_nb,  keep_miss_pm  = miss("MRM_KEEP")
    study_miss_nb, study_miss_pm = miss("MRM_STUDY")
    add_miss_nb,   add_miss_pm   = miss("MRM_ADD")

    # ── Suivi des consignes (univers MRM principal : matchés légitimes + MISSING) ─
    # EXCLUT les tardifs (CPT_LATE : consigne issue d'un autre inventaire ou absente).
    # nb / conformité : tous les dossiers principaux de la consigne.
    # PM (MRM, Compte, Δ), taux de chute, volumétrie PM nulle/non-nulle : dossiers
    # MATCHÉS seulement. Pour "à supprimer", l'analyse PM n'est pas pertinente.
    def consigne(action):
        by_action = lambda r: A(r) == action
        if action == "MRM_DELETE":
            univ = lambda r: by_action(r) and (T(r) in match or T(r) == "MRM_DELETE")
            conf = lambda r: univ(r) and T(r) not in match     # conforme = écarté
        else:
            univ = lambda r: by_action(r) and (T(r) in match or T(r) == "MRM_MISSING")
            conf = lambda r: univ(r) and T(r) in match          # conforme = retrouvé
        nb       = agg("nb", univ)
        conf_nb  = agg("nb", conf)
        # Univers PM / chute = univers chute global restreint à la consigne
        # (matchés + N+1, hors statut NON) — cohérence : global = Σ consignes
        # + hors consigne. cf. docs/METRIQUES.md §4.
        chute_c  = lambda r: by_action(r) and in_metrics(r) and not N(r)
        nb_c     = agg("nb",           chute_c)
        nb_late  = agg("nb", lambda r: chute_c(r) and T(r) == "CPT_LATE")  # part N+1
        nb_inv   = nb_c - nb_late                     # part inventaire courant
        pm_mrm_c = agg("pm_mrm",       chute_c)
        pm_cpt_c = agg("pm_cpt",       chute_c)
        nz       = agg("nb_pm_mrm_nz", chute_c)   # PM MRM ≠ 0
        nz0      = nb_c - nz                         # PM MRM nulle (null ou 0)
        delta    = pm_mrm_c - pm_cpt_c
        # Nature du KO (cf. METRIQUES.md §5.1) : pour KEEP/ADD/STUDY le non
        # matché est "non retrouvé" (absent du compte) ; pour DELETE c'est
        # l'inverse — la consigne non suivie est "encore au compte".
        ko        = nb - conf_nb
        ko_label  = "encore au compte" if action == "MRM_DELETE" else "non retrouvé"
        return {
            "nb": nb, "conf": conf_nb, "pct": _pct(conf_nb, nb),
            "ko": ko, "ko_label": ko_label,
            # Base PM / chute = retrouvés inventaire + récupérés N+1.
            "nb_match": nb_c, "nb_inv": nb_inv, "nb_late": nb_late,
            "nz": nz,   "pct_nz":  _pct(nz, nb_c),
            "nz0": nz0, "pct_nz0": _pct(nz0, nb_c),
            "pm_mrm": pm_mrm_c, "pm_cpt": pm_cpt_c, "delta": delta,
            "taux_chute": _pct(delta, pm_mrm_c),
            "pertinent": action != "MRM_DELETE",
        }

    keep   = consigne("MRM_KEEP")
    study  = consigne("MRM_STUDY")
    add    = consigne("MRM_ADD")
    delete = consigne("MRM_DELETE")

    # ── Conformité globale (KEEP+ADD+STUDY, univers principal) ────────────────
    total_kas = agg("nb", lambda r: A(r) in _KAS and (T(r) in match or T(r) == "MRM_MISSING"))
    conf_kas  = agg("nb", lambda r: A(r) in _KAS and T(r) in match)

    # ── Taux de chute (tous matchés hors « à supprimer » / statut NON) ────────
    # UNIVERS DE RÉFÉRENCE UNIQUE pour toute grandeur "globale" de chute :
    # taux, niveaux de PM et écart partagent ces mêmes composantes partout
    # (synthèse, métriques, graphiques).
    pm_mrm_chute = agg("pm_mrm", in_chute)
    pm_cpt_chute = agg("pm_cpt", in_chute)
    nb_chute     = agg("nb",     in_chute)
    # Décomposition de la base chute : matchés inventaire courant vs récupérés
    # N+1, pour expliciter que "matchés (base chute) = matchés inv. + N+1".
    nb_chute_late = agg("nb", lambda r: in_chute(r) and T(r) == "CPT_LATE")
    nb_chute_inv  = nb_chute - nb_chute_late
    global_delta = pm_mrm_chute - pm_cpt_chute
    taux_chute_global = _pct(global_delta, pm_mrm_chute)

    # Matchés sans consigne reconnue (MRM_ACTION null/inconnue) : inclus dans
    # la base chute mais hors des consignes KEEP/ADD/STUDY — tracés à part pour
    # que la réconciliation global = Σ consignes + hors consigne reste exacte.
    hors_consigne = lambda r: in_chute(r) and A(r) not in _KAS
    nb_hc     = agg("nb",     hors_consigne)
    pm_mrm_hc = agg("pm_mrm", hors_consigne)
    pm_cpt_hc = agg("pm_cpt", hors_consigne)

    # ── AUTO-CONTRÔLE : chute globale == Σ consignes KAS + hors consigne ──────
    # Global et par-consigne partagent le MÊME univers (matchés + CPT_LATE,
    # hors DELETE / statut NON) → le global doit être l'agrégat exact des
    # consignes KAS plus le bloc hors consigne. Tout écart > tolérance signale
    # une divergence d'univers (régression) : logué et remonté dans la synthèse.
    _kas_consignes = (keep, study, add)               # DELETE exclu de la chute
    sum_pm_mrm = sum(c["pm_mrm"] for c in _kas_consignes) + pm_mrm_hc
    sum_pm_cpt = sum(c["pm_cpt"] for c in _kas_consignes) + pm_cpt_hc
    sum_delta  = sum_pm_mrm - sum_pm_cpt
    taux_chute_consignes = _pct(sum_delta, sum_pm_mrm)
    _eps = 0.01                                        # tolérance € (arrondis flottants)
    chute_coherente = (
        abs(sum_pm_mrm - pm_mrm_chute) <= _eps
        and abs(sum_pm_cpt - pm_cpt_chute) <= _eps
        and abs(sum_delta  - global_delta) <= _eps
    )
    if not chute_coherente:
        logger.warning(
            "INCOHÉRENCE taux de chute global ↔ Σ consignes + hors consigne : "
            "PM_MRM %.2f≠%.2f | PM_CPT %.2f≠%.2f | écart %.2f≠%.2f | "
            "taux %.2f%%≠%.2f%%",
            pm_mrm_chute, sum_pm_mrm, pm_cpt_chute, sum_pm_cpt,
            global_delta, sum_delta, taux_chute_global, taux_chute_consignes,
        )

    nb_trouves = nb_match + nb_late   # matchés inventaire + récupérés tardifs

    # ── Invariant de cohérence ────────────────────────────────────────────────
    # Toute ligne de df_result doit tomber dans exactement une catégorie connue.
    # Les lignes matchées sont physiquement uniques (CPT joint à MRM) → on les
    # compte une fois. classified < total_rows ⇒ un TYPE_RECONCILIATION inattendu
    # (label orphelin, étape oubliée) n'est pas pris en compte par la synthèse.
    total_rows      = sum(r["nb"] for r in rows)
    classified_rows = nb_match + nb_del + nb_miss + nb_def + nb_late + nb_obs + nb_recup_non
    labels_connus   = match | {"MRM_DELETE", "MRM_MISSING", "CPT_ONLY", "CPT_LATE",
                               OBS_TARDIVE_LABEL, RECUP_NON_LABEL}
    labels_inconnus = sorted({T(r) for r in rows if T(r) not in labels_connus})

    return {
        # ── Bulle MRM ──
        "mrm_nb"          : nb_match + nb_del + nb_miss,
        "mrm_pm"          : mrm_pm_total,
        "a_supprimer_nb"  : nb_del,             "a_supprimer_pm"  : pm_del,
        "a_comparer_nb"   : nb_match + nb_miss, "a_comparer_pm"   : pm_match_mrm + pm_miss,
        "principale_nb"   : nb_princ,           "principale_pm"   : pm_princ,
        "affinee_nb"      : nb_aff,             "affinee_pm_mrm"  : pm_aff_mrm,
        "recup_nb"        : nb_recup,           "recup_pm_mrm"    : pm_recup_mrm,
        "non_mappes_nb"   : nb_miss,            "non_mappes_pm"   : pm_miss,
        "keep_nb"  : keep_miss_nb,   "keep_pm"  : keep_miss_pm,
        "study_nb" : study_miss_nb,  "study_pm" : study_miss_pm,
        "add_nb"   : add_miss_nb,    "add_pm"   : add_miss_pm,
        # ── Bulle MATCHÉS (légitimes inventaire) ──
        "match_nb"        : nb_match,
        "match_pm_mrm"    : pm_match_mrm,
        "match_pm_cpt"    : pm_match_cpt,
        "match_pm_ecart"  : pm_match_mrm - pm_match_cpt,
        # ── Bulle COMPTE ──
        "cpt_nb"          : nb_match + nb_def + nb_late + nb_obs + nb_recup_non,
        "cpt_pm"          : cpt_pm_total,
        "trouves_nb"      : nb_trouves,
        # Récupérés N+1 réels (avec contrepartie MRM, comptés dans les métriques).
        "late_nb" : nb_late, "late_pm" : pm_late_cpt,
        "late_pm_mrm" : pm_late_mrm, "late_pm_cpt" : pm_late_cpt,
        # Observations tardives IT = ANOMALIES (hors métriques, hors taux récup).
        "obs_nb"  : nb_obs,  "obs_pm"  : pm_obs_cpt,
        # Récupérés via MRM statut NON (anomalie résolue, hors métriques).
        "recup_non_nb"        : nb_recup_non,
        "recup_non_pm"        : pm_recup_non_cpt,
        "recup_non_pm_mrm"    : pm_recup_non_mrm,     # doit valoir 0 (contrôle)
        "recup_non_pm_mrm_nz" : nb_recup_non_pm_nz,   # nb dossiers PM MRM ≠ 0
        "recup_non_pm_mrm_ok" : recup_non_pm_mrm_ok,  # hypothèse NON ⇒ PM MRM = 0
        "def_nb"  : nb_def,  "def_pm"  : pm_def,
        # ── Indicateurs (taux) ──
        # Les obs tardives IT (sinistres clos avant l'inventaire suivant) sont
        # EXCLUES de tous les dénominateurs : non destinées à matcher. Le résidu
        # des taux compte = CPT_ONLY définitifs (= anomalies réelles).
        #   couverture_mrm     : matchés / à comparer MRM            (nb_match + nb_miss)
        #   couverture_compte  : matchés inventaire / compte réconciliable (match+late+def)
        #   recup_tardive      : récupérés N+1 / orphelins post-inventaire   (late+def)
        #   recup_global       : (matchés + récupérés N+1) / compte réconciliable
        "taux_couverture_mrm"     : _pct(nb_match, nb_match + nb_miss),
        "taux_couverture_compte"  : _pct(nb_match, nb_match + nb_late + nb_def),
        "taux_recup_tardive"      : _pct(nb_late, nb_late + nb_def),
        "taux_recup_global"       : _pct(nb_match + nb_late, nb_match + nb_late + nb_def),
        "taux_chute_global"       : taux_chute_global,
        "taux_chute_consignes"    : taux_chute_consignes,   # Σ chutes par consigne (contrôle)
        "chute_coherente"         : chute_coherente,        # global == Σ consignes ?
        "conformite_globale"      : _pct(conf_kas, total_kas),
        # ── Niveaux de PM — UNIVERS DU TAUX DE CHUTE GLOBAL ──
        # Mêmes composantes que taux_chute_global (tous matchés + N+1, hors
        # « à supprimer » / statut NON) : Écart / PM MRM × 100 ==
        # taux_chute_global, partout. Pas de pourcentage affiché ici (le seul
        # ratio de chute restitué est le taux de chute global + le par-consigne).
        "metrics_pm_mrm"   : pm_mrm_chute,
        "metrics_pm_cpt"   : pm_cpt_chute,
        "metrics_pm_ecart" : global_delta,
        "metrics_nb"       : nb_chute,
        "metrics_match_nb" : nb_chute_inv,    # matchés inventaire courant (base chute)
        "metrics_late_nb"  : nb_chute_late,   # récupérés N+1 inclus dans la base chute
        # Matchés sans consigne reconnue — inclus dans la base chute, hors
        # consignes KAS (réconciliation : global = Σ consignes + hors consigne).
        "hors_consigne_nb"     : nb_hc,
        "hors_consigne_pm_mrm" : pm_mrm_hc,
        "hors_consigne_pm_cpt" : pm_cpt_hc,
        # ── Suivi des consignes (détail) ──
        "consignes" : {
            "À conserver" : keep,
            "À étudier"   : study,
            "À ajouter"   : add,
            "À supprimer" : delete,
        },
        # ── Invariant de cohérence ──
        "total_rows"      : total_rows,
        "classified_rows" : classified_rows,
        "coherent"        : total_rows == classified_rows,
        "labels_inconnus" : labels_inconnus,
        # ── Entête ──
        "date_inventaire" : _resolve_date_inventaire(df_result),
    }


def kas_totaux(d: dict) -> dict:
    """Totaux KEEP+ADD+STUDY depuis les scalaires de compute_synthese.

    Sert à la conformité globale (nb, conformes). Les champs PM sont les
    Σ des consignes KAS : pour la chute GLOBALE, utiliser d["metrics_pm_*"]
    (univers = tous matchés hors « à supprimer » / statut NON, qui inclut
    aussi les dossiers sans consigne reconnue — d["hors_consigne_*"])."""
    kas = [d["consignes"][k] for k in ("À conserver", "À étudier", "À ajouter")]
    return {
        "nb"     : sum(c["nb"]     for c in kas),
        "conf"   : sum(c["conf"]   for c in kas),
        "pm_mrm" : sum(c["pm_mrm"] for c in kas),
        "pm_cpt" : sum(c["pm_cpt"] for c in kas),
        "delta"  : sum(c["delta"]  for c in kas),
    }


def _resolve_date_inventaire(df_result: DataFrame) -> str:
    """Date d'inventaire : figée (profile) ou max(MRM_D_INVENTAIRE) si 'auto'."""
    if DATE_INVENTAIRE != "auto":
        return DATE_INVENTAIRE
    if "MRM_D_INVENTAIRE" not in df_result.columns:
        return "n/d"
    row = df_result.agg(
        F.date_format(F.max("MRM_D_INVENTAIRE"), "dd/MM/yyyy").alias("d")
    ).first()
    return (row and row["d"]) or "n/d"


# ============================================================================
# RENDU ASCII
# ============================================================================

_B    = 34         # largeur d'une bulle
_LBL  = 27         # largeur du libellé dans la colonne latérale
_NBW  = 7          # largeur du nombre de dossiers (jusqu'à 9 999 999)
_PMW  = 13         # largeur de la PM (jusqu'aux milliards : "1 554 072 064")
_RW   = _LBL + 2 + _NBW + 2 + _PMW + 2   # libellé + ": " + nb + "  " + pm + " €"
_T    = 3 + _B + 3 + _RW   # largeur du contenu intérieur de la boîte


def _n(x) -> str:
    """Entier avec séparateur de milliers espace (style FR)."""
    return f"{int(round(x or 0)):,}".replace(",", " ")


def _row(label: str, nb, pm) -> str:
    """Ligne latérale : 'libellé : nb  PM €' (nb et PM alignés à droite).

    pm=None → colonne PM laissée vide (sous-total sans contrepartie PM pertinente).
    """
    pm_txt = " " * (_PMW + 2) if pm is None else f"{_n(pm):>{_PMW}} €"
    return f"{label:<{_LBL}}: {_n(nb):>{_NBW}}  {pm_txt}"


def _bubble(*lines: str) -> list:
    """Construit une mini-bulle encadrée de largeur _B."""
    inner = _B - 4
    top = "┌" + "─" * (_B - 2) + "┐"
    bot = "└" + "─" * (_B - 2) + "┘"
    body = ["│ " + ln[:inner].ljust(inner) + " │" for ln in lines]
    return [top, *body, bot]


def _render_box(d: dict, client: str) -> str:
    """Bloc principal en 3 bulles + colonne latérale détaillée."""
    left = (
        _bubble(
            f"MRM = {_n(d['mrm_nb'])} dossiers",
            f"     / PM {_n(d['mrm_pm'])} €",
        )
        + _bubble(
            f"RETROUVÉS base chute = {_n(d['metrics_nb'])}",
            f" {_n(d['metrics_match_nb'])} inv. + {_n(d['metrics_late_nb'])} N+1",
            f" PM MRM   {_n(d['metrics_pm_mrm'])} €",
            f" PM CPT   {_n(d['metrics_pm_cpt'])} €",
            f" Δ PM     {_n(d['metrics_pm_ecart'])} €",
        )
        + _bubble(
            f"COMPTE = {_n(d['cpt_nb'])} dossiers",
            f"      / PM {_n(d['cpt_pm'])} €",
        )
    )

    # « À supprimer » encore au compte (matchés DELETE) = sous-ensemble des
    # retrouvés, consigne non suivie. Affiché pour réconcilier avec la table consignes :
    #   à supprimer (absents=OK) + encore au compte (KO) = total consigne à supprimer.
    del_ko = d["consignes"]["À supprimer"]["ko"]
    right = [
        "",
        _row("À supprimer — absents (OK)", d["a_supprimer_nb"], d["a_supprimer_pm"]),
        _row("À comparer",            d["a_comparer_nb"],  d["a_comparer_pm"]),
        _row("Retrouvés clé principale", d["principale_nb"], d["principale_pm"]),
        _row("Retrouvés clé affinée",    d["affinee_nb"],    d["affinee_pm_mrm"]),
        _row("Retrouvés récupération",   d["recup_nb"],      d["recup_pm_mrm"]),
        _row("└ dont à supprimer (KO)",  del_ko,             None),
        _row("Non retrouvés au compte",  d["non_mappes_nb"], d["non_mappes_pm"]),
        _row("├ à conserver",         d["keep_nb"],        d["keep_pm"]),
        _row("├ à étudier",           d["study_nb"],       d["study_pm"]),
        _row("└ à ajouter",           d["add_nb"],         d["add_pm"]),
        "",
        _row("Total CPT (compte)",          d["cpt_nb"],     d["cpt_pm"]),
        _row("├ retrouvés (inventaire)",    d["match_nb"],   d["match_pm_cpt"]),
        _row("├ retrouvés via N+1",         d["late_nb"],    d["late_pm"]),
        _row("├ repêchés (statut MRM non)", d["recup_non_nb"], d["recup_non_pm"]),
        _row("├ clos avant inv. N+1",       d["obs_nb"],     d["obs_pm"]),
        _row("└ sans contrepartie (anom.)", d["def_nb"],     d["def_pm"]),
        "",
    ]

    top    = "┌" + "─" * _T + "┐"
    sep    = "├" + "─" * _T + "┤"
    bottom = "└" + "─" * _T + "┘"
    header = f"  Compte client : {client}".ljust(38) + f"Date inventaire : {d['date_inventaire']}"

    out = [top, "│" + header.ljust(_T) + "│", sep]
    for i in range(max(len(left), len(right))):
        l = left[i]  if i < len(left)  else " " * _B
        r = right[i] if i < len(right) else ""
        content = "   " + l + "   " + r
        out.append("│" + content.ljust(_T) + "│")
    out.append(bottom)
    return "\n".join(out)


def _render_indicateurs(d: dict) -> str:
    """Bloc des taux globaux + niveaux de PM (univers : matchés légitimes + tardifs)."""
    coh = "✔ cohérent" if d["coherent"] else "✘ INCOHÉRENT"
    detail_coh = (
        f"{_n(d['classified_rows'])} / {_n(d['total_rows'])} lignes classées"
        + ("" if d["coherent"]
           else f" — labels non pris en compte : {', '.join(d['labels_inconnus']) or 'n/d'}")
    )
    lines = [
        "LEXIQUE : retrouvé = dossier de la revue présent au compte | non retrouvé = absent du compte",
        "          conforme = consigne respectée (conserver/étudier/ajouter → retrouvé ; à supprimer → absent)",
        "",
        "INDICATEURS",
        "  COUVERTURE",
        f"    Taux de couverture MRM (retrouvés / à comparer)        : {d['taux_couverture_mrm']:>5} %",
        f"    Taux de couverture compte (retrouvés inv. / compte)   : {d['taux_couverture_compte']:>5} %",
        "  RÉCUPÉRATION (compte, déclarations tardives N+1)",
        f"    Taux de récupération tardive (retrouvés N+1 / restes) : {d['taux_recup_tardive']:>5} %",
        f"    Taux de récupération global (retrouvés + N+1 / compte): {d['taux_recup_global']:>5} %",
        "  PROVISIONNEMENT",
        f"    Taux de chute global (matchés hors suppr./statut NON)  : {d['taux_chute_global']:>5} %",
        f"      ↳ contrôle Σ consignes + hors consigne : {d['taux_chute_consignes']:>5} %  "
        + ("✔ cohérent" if d["chute_coherente"] else "✘ INCOHÉRENT (voir logs)"),
        f"    Conformité globale des consignes                       : {d['conformite_globale']:>5} %",
        "  (dénominateurs compte hors sinistres clos avant inventaire suivant)",
        "",
        f"NIVEAUX DE PM — base du taux de chute ({_n(d['metrics_nb'])} dossiers retrouvés,",
        "  inventaire + récupérés N+1, hors « à supprimer » et statut inventaire NON)",
        f"  PM MRM   : {_n(d['metrics_pm_mrm']):>15} €",
        f"  PM CPT   : {_n(d['metrics_pm_cpt']):>15} €",
        f"  Écart    : {_n(d['metrics_pm_ecart']):>15} €",
        f"  (dont {_n(d['hors_consigne_nb'])} dossiers sans consigne reconnue — "
        f"PM MRM {_n(d['hors_consigne_pm_mrm'])} €, inclus dans la base)",
        "",
        f"RÉCUPÉRATION TARDIVE N+1 ({_n(d['late_nb'])} dossiers, INCLUS dans les métriques)",
        f"  Dossiers CPT orphelins retrouvés dans l'inventaire N+1  "
        f"(PM MRM {_n(d['late_pm_mrm'])} € | PM CPT {_n(d['late_pm_cpt'])} €)",
        "",
        f"SINISTRES CLOS AVANT INVENTAIRE SUIVANT ({_n(d['obs_nb'])} dossiers, hors métriques)",
        f"  Obs. tardives IT (garantie 60, fin d'année) : sinistre clos avant l'inventaire",
        f"  MRM N+1 → non retrouvé (explicable, pas une anomalie). PM CPT {_n(d['obs_pm'])} €.",
        "",
        f"RÉCUPÉRÉS VIA MRM STATUT NON ({_n(d['recup_non_nb'])} dossiers, hors métriques)",
        f"  CPT_ONLY repêchés sur un MRM statut NON (PM MRM = 0, non remonté à la",
        f"  direction financière) : anomalie résolue. PM CPT {_n(d['recup_non_pm'])} €. Voir analyse dédiée.",
        f"  ↳ contrôle PM MRM = 0 : "
        + ("✔ vérifié" if d["recup_non_pm_mrm_ok"]
           else f"✘ VIOLÉ — {_n(d['recup_non_pm_mrm_nz'])} dossier(s), "
                f"PM MRM {_n(d['recup_non_pm_mrm'])} € (voir logs)"),
        "",
        f"ANOMALIES — CPT_ONLY définitifs ({_n(d['def_nb'])} dossiers, PM CPT {_n(d['def_pm'])} €)",
        f"  Dossiers compte sans contrepartie MRM, ni récupérés, ni explicables.",
        "",
        f"CONTRÔLE DE COHÉRENCE : {coh} — {detail_coh}",
    ]
    return "\n".join(lines)


def _np(n, p) -> str:
    """Formate 'n (p%)' — volumétrie avec pourcentage entre parenthèses."""
    return f"{_n(n)} ({p}%)"


def _render_consignes(d: dict) -> str:
    """
    Suivi des consignes — deux univers explicites et réconciliables :

      CONFORMITÉ (inventaire courant)  : total = retrouvés + reste ; conformes ;
        %conf = conformes / total ; reste = non retrouvé (conserver/étudier/
        ajouter absents du compte) ou encore au compte (à supprimer non suivie).
      PROVISIONNEMENT (inventaire + N+1) : base = dossiers retrouvés servant à la
        PM et au taux de chute (dont la part récupérée N+1) ; PM MRM, PM CPT,
        chute. "À supprimer" → PM non pertinente.

    Les deux univers partagent les retrouvés de l'inventaire ; ils diffèrent de
    la part N+1 (colonne « dont N+1 ») côté PM et des « non retrouvés » côté
    conformité — d'où total ≠ base, désormais tracé colonne par colonne.
    """
    head = (f"  {'Consigne':<13}│{'total':>7}{'conformes':>11}{'%conf':>8}"
            f"{'reste (statut)':>22}  │{'base':>7}{'dont N+1':>9}"
            f"{'PM MRM':>16}{'PM CPT':>16}{'chute':>8}")
    sep  = "  " + "─" * (len(head) - 2)
    lines = [
        "SUIVI DES CONSIGNES",
        "  CONFORMITÉ : univers inventaire courant (retrouvés vs non) — conformes / total.",
        "  PROVISIONNEMENT : PM & taux de chute sur les dossiers retrouvés (inventaire + récupérés N+1).",
        "  Reste : conserver/étudier/ajouter = non retrouvé (absent du compte) ; à supprimer = encore au compte.",
        head,
        sep,
    ]
    for label, c in d["consignes"].items():
        statut = f"{_n(c['ko'])} {c['ko_label']}" if c["ko"] else "—"
        left = (f"  {label:<13}│{_n(c['nb']):>7}{_n(c['conf']):>11}{c['pct']:>6} %"
                f"{statut:>22}  │")
        if not c["pertinent"]:
            lines.append(left + f"{_n(c['nb_match']):>7}{_n(c['nb_late']):>9}"
                         + "      — PM non pertinente (à supprimer) —")
        else:
            lines.append(
                left
                + f"{_n(c['nb_match']):>7}{_n(c['nb_late']):>9}"
                + f"{_n(c['pm_mrm']):>14} €{_n(c['pm_cpt']):>14} €{c['taux_chute']:>6} %"
            )
    return "\n".join(lines)


def render_synthese(d: dict, client: str = CLIENT_NAME) -> str:
    """Rend la synthèse complète : box + indicateurs + consignes."""
    return "\n\n".join([
        _render_box(d, client),
        _render_indicateurs(d),
        _render_consignes(d),
    ])


@timed_fn("print_synthese")
def print_synthese(df_result: DataFrame) -> dict:
    """Calcule + affiche la synthèse. Retourne les scalaires."""
    d = compute_synthese(df_result)
    print("\n" + render_synthese(d) + "\n")
    return d
