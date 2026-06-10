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

    Univers MÉTRIQUES (taux de chute, niveaux de PM) = MATCHÉS + récupérés N+1.
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

    rows = (
        df.groupBy("TYPE_RECONCILIATION", "MRM_ACTION", "LATE_SOURCE")
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
    in_metrics     = lambda r: T(r) in match or T(r) == "CPT_LATE"
    pm_metrics_mrm = pm_match_mrm + pm_late_mrm
    pm_metrics_cpt = pm_match_cpt + pm_late_cpt
    nb_metrics     = nb_match + nb_late

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
        # Univers PM / chute = matchés + récupérés N+1 (CPT_LATE), identique au
        # taux de chute global et à la table d'analyse taux_chute (cohérence :
        # global = Σ consignes). cf. docs/METRIQUES.md §4.
        in_chute = lambda r: by_action(r) and in_metrics(r)
        nb_c     = agg("nb",           in_chute)
        pm_mrm_c = agg("pm_mrm",       in_chute)
        pm_cpt_c = agg("pm_cpt",       in_chute)
        nz       = agg("nb_pm_mrm_nz", in_chute)   # PM MRM ≠ 0
        nz0      = nb_c - nz                         # PM MRM nulle (null ou 0)
        delta    = pm_mrm_c - pm_cpt_c
        return {
            "nb": nb, "conf": conf_nb, "pct": _pct(conf_nb, nb),
            "nb_match": nb_c,
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

    # ── Taux de chute (KEEP/ADD/STUDY, univers métriques = matchés + tardifs) ─
    in_kas_metrics = lambda r: A(r) in _KAS and in_metrics(r)
    pm_mrm_kas = agg("pm_mrm", in_kas_metrics)
    pm_cpt_kas = agg("pm_cpt", in_kas_metrics)
    global_delta = pm_mrm_kas - pm_cpt_kas
    taux_chute_global = _pct(global_delta, pm_mrm_kas)

    # ── AUTO-CONTRÔLE : taux de chute global == Σ des chutes par consigne ──────
    # Global et par-consigne partagent désormais le MÊME univers (matchés +
    # CPT_LATE, KEEP/ADD/STUDY) → le global doit être l'agrégat exact des
    # consignes. Tout écart > tolérance signale une divergence d'univers
    # (régression) : on le logue et on le remonte dans la synthèse.
    _kas_consignes = (keep, study, add)               # DELETE exclu de la chute
    sum_pm_mrm = sum(c["pm_mrm"] for c in _kas_consignes)
    sum_pm_cpt = sum(c["pm_cpt"] for c in _kas_consignes)
    sum_delta  = sum(c["delta"]  for c in _kas_consignes)
    taux_chute_consignes = _pct(sum_delta, sum_pm_mrm)
    _eps = 0.01                                        # tolérance € (arrondis flottants)
    chute_coherente = (
        abs(sum_pm_mrm - pm_mrm_kas) <= _eps
        and abs(sum_pm_cpt - pm_cpt_kas) <= _eps
        and abs(sum_delta  - global_delta) <= _eps
    )
    if not chute_coherente:
        logger.warning(
            "INCOHÉRENCE taux de chute global ↔ Σ consignes : "
            "PM_MRM %.2f≠%.2f | PM_CPT %.2f≠%.2f | écart %.2f≠%.2f | "
            "taux %.2f%%≠%.2f%%",
            pm_mrm_kas, sum_pm_mrm, pm_cpt_kas, sum_pm_cpt,
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
        # ── Niveaux de PM (matchés légitimes + tardifs) ──
        "metrics_pm_mrm"   : pm_metrics_mrm,
        "metrics_pm_cpt"   : pm_metrics_cpt,
        "metrics_pm_ecart" : pm_metrics_mrm - pm_metrics_cpt,
        "metrics_pm_pct"   : _pct(pm_metrics_mrm - pm_metrics_cpt, pm_metrics_mrm),
        "metrics_nb"       : nb_metrics,
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


def build_synthese_indicateurs(df_result: DataFrame) -> DataFrame:
    """
    Table d'indicateurs de synthèse — UNE ligne par run.

    Aplati les scalaires de compute_synthese (taux de couverture, récupération
    N+1, chute global, conformité, niveaux de PM, volumétries) en un DataFrame
    d'une ligne : historisable en base (suivi dans le temps) et directement
    lisible en présentation. Périmètre + date d'inventaire en tête.
    """
    d = compute_synthese(df_result)
    row = {
        "PERIMETRE"             : CLIENT_NAME,
        "DATE_INVENTAIRE"       : d["date_inventaire"],
        "NB_MATCHES"            : int(d["match_nb"]),
        "NB_RECUP_N1"           : int(d["late_nb"]),
        "NB_RECUP_STATUT_NON"   : int(d["recup_non_nb"]),
        "PM_CPT_RECUP_NON"      : float(d["recup_non_pm"]),
        "RECUP_NON_PM_MRM_OK"   : bool(d["recup_non_pm_mrm_ok"]),
        "NB_CPT_ONLY"           : int(d["def_nb"]),
        "NB_MRM_MISSING"        : int(d["non_mappes_nb"]),
        "NB_A_SUPPRIMER"        : int(d["a_supprimer_nb"]),
        "TAUX_COUVERTURE_MRM"   : float(d["taux_couverture_mrm"]),
        "TAUX_COUVERTURE_COMPTE": float(d["taux_couverture_compte"]),
        "TAUX_RECUP_TARDIVE"    : float(d["taux_recup_tardive"]),
        "TAUX_RECUP_GLOBAL"     : float(d["taux_recup_global"]),
        "TAUX_CHUTE_GLOBAL"     : float(d["taux_chute_global"]),
        "CHUTE_COHERENTE"       : bool(d["chute_coherente"]),
        "CONFORMITE_GLOBALE"    : float(d["conformite_globale"]),
        "PM_MRM"                : float(d["metrics_pm_mrm"]),
        "PM_CPT"                : float(d["metrics_pm_cpt"]),
        "PM_ECART"              : float(d["metrics_pm_ecart"]),
        "PM_ECART_PCT"          : float(d["metrics_pm_pct"]),
        "COHERENT"              : bool(d["coherent"]),
    }
    # Schéma explicite → ordre de colonnes stable d'un run à l'autre.
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, DoubleType, BooleanType,
    )
    _t = {bool: BooleanType(), int: LongType(), float: DoubleType(), str: StringType()}
    schema = StructType([StructField(k, _t[type(v)], True) for k, v in row.items()])
    return df_result.sparkSession.createDataFrame([tuple(row.values())], schema=schema)


def kas_totaux(d: dict) -> dict:
    """Totaux KEEP+ADD+STUDY depuis les scalaires de compute_synthese.

    Sert aux ratios globaux et aux graphiques KPI : conformité (nb, conformes)
    et chute (PM MRM, PM CPT, écart) — mêmes univers que la synthèse, donc
    réconciliables avec les onglets suivi_consignes / taux_chute."""
    kas = [d["consignes"][k] for k in ("À conserver", "À étudier", "À ajouter")]
    return {
        "nb"     : sum(c["nb"]     for c in kas),
        "conf"   : sum(c["conf"]   for c in kas),
        "pm_mrm" : sum(c["pm_mrm"] for c in kas),
        "pm_cpt" : sum(c["pm_cpt"] for c in kas),
        "delta"  : sum(c["delta"]  for c in kas),
    }


def build_ratios_globaux(df_result: DataFrame) -> DataFrame:
    """
    Ratios GLOBAUX de restitution — une ligne par indicateur, avec NUMÉRATEUR
    et DÉNOMINATEUR explicites (les chiffres se réconcilient avec la synthèse
    et les onglets taux_chute / suivi_consignes). Livrable direct pour les
    clients internes (direction financière, engagements).

    Colonnes : PERIMETRE, DATE_INVENTAIRE, INDICATEUR, VALEUR_PCT,
               NUMERATEUR, DENOMINATEUR, UNITE, LECTURE.
    """
    d = compute_synthese(df_result)
    k = kas_totaux(d)
    m, l, anom, miss = d["match_nb"], d["late_nb"], d["def_nb"], d["non_mappes_nb"]
    rows = [
        ("Taux de chute global (conserver/étudier/ajouter)", d["taux_chute_global"],
         round(k["delta"], 2), round(k["pm_mrm"], 2), "€",
         "Σ(PM MRM − PM CPT) / Σ PM MRM, matchés + récupérés N+1 ; > 0 = sous-provisionné (risque)"),
        ("Conformité globale des consignes", d["conformite_globale"],
         float(k["conf"]), float(k["nb"]), "dossiers",
         "consignes conserver/étudier/ajouter retrouvées au compte / total de ces consignes"),
        ("Taux de couverture MRM", d["taux_couverture_mrm"],
         float(m), float(m + miss), "dossiers",
         "matchés / revue à comparer (consignes à supprimer exclues)"),
        ("Taux de couverture compte", d["taux_couverture_compte"],
         float(m), float(m + l + anom), "dossiers",
         "matchés inventaire courant / compte réconciliable"),
        ("Taux de récupération tardive N+1", d["taux_recup_tardive"],
         float(l), float(l + anom), "dossiers",
         "orphelins retrouvés dans l'inventaire N+1 / orphelins post-inventaire"),
        ("Taux de récupération global", d["taux_recup_global"],
         float(m + l), float(m + l + anom), "dossiers",
         "(matchés + récupérés N+1) / compte réconciliable"),
    ]
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    schema = StructType([
        StructField("PERIMETRE",       StringType(), True),
        StructField("DATE_INVENTAIRE", StringType(), True),
        StructField("INDICATEUR",      StringType(), True),
        StructField("VALEUR_PCT",      DoubleType(), True),
        StructField("NUMERATEUR",      DoubleType(), True),
        StructField("DENOMINATEUR",    DoubleType(), True),
        StructField("UNITE",           StringType(), True),
        StructField("LECTURE",         StringType(), True),
    ])
    data = [(CLIENT_NAME, d["date_inventaire"], n, float(v), float(num), float(den), u, lec)
            for n, v, num, den, u, lec in rows]
    return df_result.sparkSession.createDataFrame(data, schema=schema)


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

_B   = 34          # largeur d'une bulle
_LBL = 27          # largeur du libellé dans la colonne latérale
_RW  = 50          # largeur de la colonne latérale (libellé + nb + PM)
_T   = 3 + _B + 3 + _RW   # largeur du contenu intérieur de la boîte


def _n(x) -> str:
    """Entier avec séparateur de milliers espace (style FR)."""
    return f"{int(round(x or 0)):,}".replace(",", " ")


def _row(label: str, nb, pm) -> str:
    """Ligne latérale : 'libellé : nb  PM €' (nb et PM alignés à droite).

    pm=None → colonne PM laissée vide (sous-total sans contrepartie PM pertinente).
    """
    pm_txt = " " * 13 if pm is None else f"{_n(pm):>11} €"
    return f"{label:<{_LBL}}: {_n(nb):>6}  {pm_txt}"


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
            f"MATCHÉS = {_n(d['match_nb'])} ({_n(d['principale_nb'])}+{_n(d['affinee_nb'])}+{_n(d['recup_nb'])})",
            f" PM MRM   {_n(d['match_pm_mrm'])} €",
            f" PM CPT   {_n(d['match_pm_cpt'])} €",
            f" Δ PM     {_n(d['match_pm_ecart'])} €",
        )
        + _bubble(
            f"COMPTE = {_n(d['cpt_nb'])} dossiers",
            f"      / PM {_n(d['cpt_pm'])} €",
        )
    )

    right = [
        "",
        _row("Consigne à supprimer",  d["a_supprimer_nb"], d["a_supprimer_pm"]),
        _row("À comparer",            d["a_comparer_nb"],  d["a_comparer_pm"]),
        _row("Mappés clé principale", d["principale_nb"],  d["principale_pm"]),
        _row("Mappés clé affinée",    d["affinee_nb"],     d["affinee_pm_mrm"]),
        _row("Mappés récupération",   d["recup_nb"],       d["recup_pm_mrm"]),
        _row("Non mappés (MISSING)",  d["non_mappes_nb"],  d["non_mappes_pm"]),
        _row("├ à conserver",         d["keep_nb"],        d["keep_pm"]),
        _row("├ à étudier",           d["study_nb"],       d["study_pm"]),
        _row("└ à ajouter",           d["add_nb"],         d["add_pm"]),
        "",
        _row("Total CPT (compte)",          d["cpt_nb"],     d["cpt_pm"]),
        _row("├ matchés inventaire",        d["match_nb"],   d["match_pm_cpt"]),
        _row("├ récupérés N+1",             d["late_nb"],    d["late_pm"]),
        _row("├ récupérés via NON",         d["recup_non_nb"], d["recup_non_pm"]),
        _row("├ clos avant inv. N+1",       d["obs_nb"],     d["obs_pm"]),
        _row("└ CPT_ONLY (anomalies)",      d["def_nb"],     d["def_pm"]),
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
        "INDICATEURS",
        "  COUVERTURE",
        f"    Taux de couverture MRM (matchés / à comparer MRM)      : {d['taux_couverture_mrm']:>5} %",
        f"    Taux de couverture compte (matchés inventaire / compte): {d['taux_couverture_compte']:>5} %",
        "  RÉCUPÉRATION (compte, déclarations tardives N+1)",
        f"    Taux de récupération tardive (récupérés / orphelins)   : {d['taux_recup_tardive']:>5} %",
        f"    Taux de récupération global (matchés + N+1 / compte)   : {d['taux_recup_global']:>5} %",
        "  PROVISIONNEMENT",
        f"    Taux de chute global (KEEP/ADD/STUDY)                  : {d['taux_chute_global']:>5} %",
        f"      ↳ contrôle Σ par consigne : {d['taux_chute_consignes']:>5} %  "
        + ("✔ cohérent" if d["chute_coherente"] else "✘ INCOHÉRENT (voir logs)"),
        f"    Conformité globale des consignes                       : {d['conformite_globale']:>5} %",
        "  (dénominateurs compte hors sinistres clos avant inventaire suivant)",
        "",
        f"NIVEAUX DE PM (matchés + récupérés N+1, {_n(d['metrics_nb'])} dossiers)",
        f"  PM MRM   : {_n(d['metrics_pm_mrm']):>15} €",
        f"  PM CPT   : {_n(d['metrics_pm_cpt']):>15} €",
        f"  Écart    : {_n(d['metrics_pm_ecart']):>15} €  ({d['metrics_pm_pct']} %)",
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
    Suivi des consignes :
      - nb / %conf : sur tous les dossiers de la consigne
      - matchés, PM nulle/non-nulle (avec %), PM MRM/CPT, taux de chute :
        sur les dossiers matchés. "À supprimer" → analyse PM non pertinente.
    """
    head = (f"  {'Consigne':<13}{'nb':>6}{'%conf':>8}{'match.':>7}"
            f"{'PM MRM nulle':>13}{'PM MRM≠0':>14}{'PM MRM':>15}{'PM CPT':>15}{'chute':>8}")
    lines = [
        "SUIVI DES CONSIGNES — conformité (tous dossiers) ; PM & chute (matchés + récupérés N+1)",
        head,
    ]
    for label, c in d["consignes"].items():
        base = f"  {label:<13}{_n(c['nb']):>6}{c['pct']:>6} %{_n(c['nb_match']):>7}"
        if not c["pertinent"]:
            lines.append(base + "   — analyse PM non pertinente (consigne à supprimer) —")
        else:
            lines.append(
                base
                + f"{_np(c['nz0'], c['pct_nz0']):>13}{_np(c['nz'], c['pct_nz']):>14}"
                + f"{_n(c['pm_mrm']):>13} €{_n(c['pm_cpt']):>13} €{c['taux_chute']:>6} %"
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
