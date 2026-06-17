"""
Dérivation PURE-PYTHON des scalaires de la synthèse (depuis les lignes
agrégées par kpi_export._collect_rows). AUCUNE dépendance Spark : chaque
ligne se lit par r["clé"] (un Row Spark comme un dict) → testable hors
cluster avec de simples dicts (cf. tests/test_scalars_from_rows.py).

Séparé de kpi_export (qui porte la passe Spark + le rendu) pour casser la
god-function : ici la logique métier, testable par morceaux.
"""

import logging

from config import (
    MATCH_PRINCIPALE,
    MATCH_AFFINEE,
    MATCH_RECUPERATION,
    MATCH_CLAUSE,
    MATCH_LABELS,
    OBS_TARDIVE_LABEL,
    RECUP_NON_LABEL,
)
from core.synthese_contract import SyntheseScalars

logger = logging.getLogger(__name__)


_KAS = ("MRM_KEEP", "MRM_ADD", "MRM_STUDY")   # consignes "à comparer" (hors DELETE)


def _pct(num, den) -> float:
    """Pourcentage arrondi à 0.1, 0.0 si dénominateur nul."""
    return round(num / den * 100, 1) if den else 0.0


def _scalars_from_rows(rows: list, date_inventaire: str) -> SyntheseScalars:
    """Dérive les 75 scalaires de la synthèse depuis les lignes agrégées (sortie
    de `_collect_rows`). PURE PYTHON, aucune dépendance Spark : chaque ligne est
    lue par `r["clé"]` (un Row Spark se lit comme un dict) → testable hors cluster
    avec de simples dicts (cf. tests/test_scalars_from_rows.py)."""
    princ    = set(MATCH_PRINCIPALE)
    aff      = set(MATCH_AFFINEE)
    recup    = set(MATCH_RECUPERATION)
    clause   = set(MATCH_CLAUSE)       # clé de secours (RPP nul / mal renseigné)
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
    # Clé de secours « clause » : matchs légitimes (vraie contrepartie MRM) posés
    # quand le RPP compte est nul / mal renseigné. Bucket distinct → ligne dédiée
    # dans bilan_cas pour auditer les éventuels faux positifs (clé moins stricte).
    nb_clause, pm_clause_mrm = mrm(lambda r: T(r) in clause)
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
    # Ventilation par exercice via LATE_SOURCE : STATUT_NON = exercice N,
    # STATUT_NON_N1 = exercice N+1 (cf. main.py, passe de repêchage). L'agrégat
    # nb_recup_non = N + N+1 reste utilisé par la bulle COMPTE / viz.
    nb_recup_non, pm_recup_non_cpt = cpt(lambda r: T(r) == RECUP_NON_LABEL)
    nb_recup_non_n,  pm_recup_non_n_cpt  = cpt(
        lambda r: T(r) == RECUP_NON_LABEL and S(r) == "STATUT_NON")
    nb_recup_non_n1, pm_recup_non_n1_cpt = cpt(
        lambda r: T(r) == RECUP_NON_LABEL and S(r) == "STATUT_NON_N1")

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

    # Matchés légitimes de l'inventaire courant (clé clause incluse — c'est un
    # vrai match, posé via une clé de secours). nb_match DOIT sommer tous les
    # buckets, sinon l'invariant classified_rows == total_rows casse.
    nb_match     = nb_princ + nb_aff + nb_recup + nb_clause
    pm_match_mrm = agg("pm_mrm", lambda r: T(r) in match)
    pm_match_cpt = agg("pm_cpt", lambda r: T(r) in match)

    # Univers CHUTE (stats globales) = matchés de l'inventaire courant, hors
    # consigne « à supprimer » et hors statut inventaire NON ; les sans-consigne
    # reconnue restent inclus (A(r) null ≠ "MRM_DELETE"). Les récupérés N+1
    # sont une ANALYSE SÉPARÉE (leur propre taux + suivi de consignes), HORS
    # stats globales ; les « à supprimer » retrouvées et les repêchés statut
    # NON sont analysés à part.
    in_chute    = lambda r: T(r) in match and A(r) != "MRM_DELETE" and not N(r)
    in_chute_n1 = lambda r: T(r) == "CPT_LATE" and A(r) != "MRM_DELETE" and not N(r)

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

    # ── Suivi des consignes (EXERCICE COURANT pur : matchés + MISSING) ───────
    # Les récupérés N+1 n'y participent PAS : leur consigne vient de
    # l'inventaire N+1, pas de la revue auditée — leur attribuer une
    # conformité ou une chute ici reviendrait à prêter à la revue courante des
    # décisions qu'elle n'a pas prises. Ils ont leur suivi séparé
    # (n1_consignes) et leur taux de chute séparé (taux_chute_n1).
    # nb / conformité : dossiers de la consigne dans la revue courante.
    # PM (MRM, Compte, Δ), taux de chute, volumétrie PM nulle/non-nulle :
    # matchés inventaire courant. Pour "à supprimer", PM non pertinente.
    def consigne(action):
        by_action = lambda r: A(r) == action
        if action == "MRM_DELETE":
            univ = lambda r: by_action(r) and (T(r) in match or T(r) == "MRM_DELETE")
            conf = lambda r: univ(r) and T(r) == "MRM_DELETE"   # conforme = écarté
        else:
            univ = lambda r: by_action(r) and (T(r) in match or T(r) == "MRM_MISSING")
            conf = lambda r: univ(r) and T(r) in match          # conforme = retrouvé
        nb       = agg("nb", univ)
        conf_nb  = agg("nb", conf)
        # Univers PM / chute = matchés inventaire courant de la consigne (hors
        # statut NON) — cohérence : taux_chute_inventaire = Σ consignes + hors
        # consigne. cf. docs/METRIQUES.md §4.
        chute_c  = lambda r: by_action(r) and T(r) in match and not N(r)
        nb_c     = agg("nb",           chute_c)
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
            # Base PM / chute = matchés inventaire courant.
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

    # ── Conformité globale (KEEP+ADD+STUDY, exercice courant) ────────────────
    total_kas = agg("nb", lambda r: A(r) in _KAS and (T(r) in match or T(r) == "MRM_MISSING"))
    conf_kas  = agg("nb", lambda r: A(r) in _KAS and T(r) in match)

    # ── Taux de chute (matchés inventaire courant hors « à supprimer » / NON) ─
    # UNIVERS DE RÉFÉRENCE UNIQUE pour toute grandeur globale de chute : taux,
    # niveaux de PM et écart partagent ces mêmes composantes partout
    # (synthèse, métriques, graphiques).
    pm_mrm_chute = agg("pm_mrm", in_chute)
    pm_cpt_chute = agg("pm_cpt", in_chute)
    nb_chute     = agg("nb",     in_chute)
    chute_delta  = pm_mrm_chute - pm_cpt_chute
    taux_chute_inv = _pct(chute_delta, pm_mrm_chute)

    # Récupérés N+1 : analyse séparée, HORS stats globales — leur propre taux.
    nb_n1     = agg("nb",     in_chute_n1)
    pm_mrm_n1 = agg("pm_mrm", in_chute_n1)
    pm_cpt_n1 = agg("pm_cpt", in_chute_n1)
    taux_chute_n1 = _pct(pm_mrm_n1 - pm_cpt_n1, pm_mrm_n1)

    # Suivi des consignes des récupérés N+1 (analyse séparée) : ventilation
    # des CPT_LATE par consigne N+1. KEEP/ADD/STUDY = conformes (retrouvés) ;
    # DELETE = encore au compte ; consigne non reconnue comptée à part.
    def _n1_nb(action):
        return agg("nb", lambda r: T(r) == "CPT_LATE" and not N(r) and A(r) == action)
    n1_consignes = {
        "À conserver" : _n1_nb("MRM_KEEP"),
        "À étudier"   : _n1_nb("MRM_STUDY"),
        "À ajouter"   : _n1_nb("MRM_ADD"),
        "À supprimer" : _n1_nb("MRM_DELETE"),
    }
    n1_sans_consigne = agg("nb", lambda r: T(r) == "CPT_LATE" and not N(r)
                           and A(r) not in _KAS and A(r) != "MRM_DELETE")

    # Base chute hors consignes KEEP/ADD/STUDY : matchés courants sans
    # consigne reconnue (MRM_ACTION null/inconnue — DELETE exclu de la base).
    # Tracés à part pour la réconciliation taux_chute_inventaire =
    # Σ consignes + hors consigne ; l'équivalent N+1 est n1_sans_consigne.
    hors_consigne = lambda r: in_chute(r) and A(r) not in _KAS
    nb_hc     = agg("nb",     hors_consigne)
    pm_mrm_hc = agg("pm_mrm", hors_consigne)
    pm_cpt_hc = agg("pm_cpt", hors_consigne)

    # ── AUTO-CONTRÔLE : chute == Σ consignes KAS + hors consigne ─────────────
    # Le taux de chute et le par-consigne partagent le MÊME univers (matchés
    # inventaire courant, hors DELETE / statut NON) → le taux doit être
    # l'agrégat exact des consignes KAS plus le bloc hors consigne. Tout écart
    # > tolérance signale une divergence d'univers (régression) : logué et
    # remonté dans la synthèse.
    _kas_consignes = (keep, study, add)               # DELETE exclu de la chute
    sum_pm_mrm = sum(c["pm_mrm"] for c in _kas_consignes) + pm_mrm_hc
    sum_pm_cpt = sum(c["pm_cpt"] for c in _kas_consignes) + pm_cpt_hc
    sum_delta  = sum_pm_mrm - sum_pm_cpt
    taux_chute_consignes = _pct(sum_delta, sum_pm_mrm)
    _eps = 0.01                                        # tolérance € (arrondis flottants)
    chute_coherente = (
        abs(sum_pm_mrm - pm_mrm_chute) <= _eps
        and abs(sum_pm_cpt - pm_cpt_chute) <= _eps
    )
    if not chute_coherente:
        logger.warning(
            "INCOHÉRENCE taux de chute : base %.2f/%.2f ≠ Σ consignes + hors "
            "consigne %.2f/%.2f (PM MRM/CPT)",
            pm_mrm_chute, pm_cpt_chute, sum_pm_mrm, sum_pm_cpt,
        )

    # RETROUVÉS = tous les matchés (inventaire courant, consignes confondues)
    # + tous les récupérés N+1. C'est la bulle centrale de la synthèse ; la
    # base du taux de chute (hors « à supprimer » / statut NON) en est un
    # sous-ensemble, détaillé dans le bloc NIVEAUX DE PM.
    nb_trouves     = nb_match + nb_late
    pm_trouves_mrm = pm_match_mrm + pm_late_mrm
    pm_trouves_cpt = pm_match_cpt + pm_late_cpt

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
        # Clé de secours « clause » (RPP nul / mal renseigné) — bucket de matchés
        # à part, audité dans bilan_cas.
        "clause_nb"       : nb_clause,          "clause_pm"       : pm_clause_mrm,
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
        "trouves_pm_mrm"  : pm_trouves_mrm,
        "trouves_pm_cpt"  : pm_trouves_cpt,
        # Récupérés N+1 réels (avec contrepartie MRM, comptés dans les métriques).
        "late_nb" : nb_late, "late_pm" : pm_late_cpt,
        "late_pm_mrm" : pm_late_mrm, "late_pm_cpt" : pm_late_cpt,
        # Observations tardives IT = ANOMALIES (hors métriques, hors taux récup).
        "obs_nb"  : nb_obs,  "obs_pm"  : pm_obs_cpt,
        # Récupérés via MRM statut NON (anomalie résolue, hors métriques).
        "recup_non_nb"        : nb_recup_non,
        "recup_non_pm"        : pm_recup_non_cpt,
        # Ventilation par exercice (info : ils sont au compte, PM MRM = 0).
        "recup_non_n_nb"      : nb_recup_non_n,   "recup_non_n_pm"  : pm_recup_non_n_cpt,
        "recup_non_n1_nb"     : nb_recup_non_n1,  "recup_non_n1_pm" : pm_recup_non_n1_cpt,
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
        "taux_chute_inventaire"   : taux_chute_inv,
        "taux_chute_consignes"    : taux_chute_consignes,   # Σ chutes par consigne (contrôle)
        "chute_coherente"         : chute_coherente,        # chute == Σ consignes ?
        "conformite_globale"      : _pct(conf_kas, total_kas),
        # ── Niveaux de PM — UNIVERS DU TAUX DE CHUTE (stats globales) ──
        # Mêmes composantes que taux_chute_inventaire (matchés inventaire
        # courant, hors « à supprimer » / statut NON) : Écart / PM MRM × 100 ==
        # taux_chute_inventaire, partout. Pas de pourcentage affiché ici (les
        # ratios de chute restitués sont le taux + le par-consigne).
        "metrics_pm_mrm"   : pm_mrm_chute,
        "metrics_pm_cpt"   : pm_cpt_chute,
        "metrics_pm_ecart" : chute_delta,
        "metrics_nb"       : nb_chute,
        # Base chute hors consignes KAS : sans consigne reconnue
        # (réconciliation : chute = Σ consignes + hors consigne).
        "hors_consigne_nb"     : nb_hc,
        "hors_consigne_pm_mrm" : pm_mrm_hc,
        "hors_consigne_pm_cpt" : pm_cpt_hc,
        # Récupérés N+1 : analyse séparée, HORS stats globales (taux + suivi
        # de consignes propres).
        "chute_n1_nb"           : nb_n1,
        "chute_n1_pm_mrm"       : pm_mrm_n1,
        "chute_n1_pm_cpt"       : pm_cpt_n1,
        "taux_chute_n1"         : taux_chute_n1,
        "n1_consignes"          : n1_consignes,
        "n1_sans_consigne"      : n1_sans_consigne,
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
        "date_inventaire" : date_inventaire,
    }
