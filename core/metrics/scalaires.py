"""
Métriques scalaires — reshape du dict de compute_synthese en tables pandas.

Chaque fonction prend `d` (SyntheseScalars — LA passe Spark, déjà faite) et
renvoie un DataFrame pandas tidy en DONNÉES BRUTES (le formatage M€/% reste
au niveau restitution). Correspondance graphiques : cf. core/metrics/__init__.py.
"""

import pandas as pd

from core.synthese.kpi_export import kas_totaux
from core.synthese.synthese_contract import SyntheseScalars
from core.metrics.base import EXERCICE_INV, EXERCICE_N1


# ============================================================================
# MÉTRIQUES SCALAIRES (reshape du dict de compute_synthese)
# ============================================================================

def synthese(d: SyntheseScalars) -> pd.DataFrame:
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
        # Clé de secours « clause » (RPP nul / mal renseigné) — sous-ensemble des
        # matchés, suivi à part pour audit.
        "NB_MATCH_CLAUSE"        : d["clause_nb"],
        "PM_MATCH_CLAUSE"        : d["clause_pm"],
        "NB_RECUP_N1"            : d["late_nb"],
        # Repêchés statut NON ventilés par exercice (hors métriques, PM MRM = 0).
        "NB_RECUP_NON_N"         : d["recup_non_n_nb"],
        "NB_RECUP_NON_N1"        : d["recup_non_n1_nb"],
        "NB_CPT_ONLY"            : d["def_nb"],
        "NB_MRM_MISSING"         : d["non_mappes_nb"],
        "NB_NON_RETROUVE"        : (cons["À conserver"]["ko"] + cons["À ajouter"]["ko"]
                                    + cons["À étudier"]["ko"]),
        "NB_ENCORE_AU_COMPTE"    : cons["À supprimer"]["ko"],
        "COHERENT"               : d["coherent"],
    }])


def bilan_cas(d: SyntheseScalars) -> pd.DataFrame:
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
        ("Retrouvés — inventaire courant", "Clé clause (RPP absent/non fiable)",
         d["clause_nb"], d["clause_pm"], None, None,
         "retrouvé via la clé de secours (n° de clause à la place du RPP) — "
         "RPP compte nul ou mal renseigné"),
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
        ("Retrouvés par tentatives", "Repêchés statut NON — exercice N",
         d["recup_non_n_nb"], None, d["recup_non_n_pm"], None,
         "anomalie résolue sur un MRM statut NON de l'exercice courant "
         "(PM MRM = 0, non remontée) — hors métriques"),
        ("Retrouvés par tentatives", "Repêchés statut NON — exercice N+1",
         d["recup_non_n1_nb"], None, d["recup_non_n1_pm"], None,
         "anomalie résolue sur un MRM statut NON de l'inventaire suivant "
         "(PM MRM = 0, non remontée) — hors métriques"),
        ("Retrouvés par tentatives", "└ total repêchés statut NON",
         d["recup_non_nb"], d["recup_non_pm_mrm"], d["recup_non_pm"], None,
         "total N + N+1 — PM MRM = 0 par construction, hors métriques"),
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


def taux_chute(d: SyntheseScalars) -> pd.DataFrame:
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


def chute_par_exercice(d: SyntheseScalars) -> pd.DataFrame:
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


def suivi_n1(d: SyntheseScalars) -> pd.DataFrame:
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


def consignes(d: SyntheseScalars) -> pd.DataFrame:
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


def compte_justification(d: SyntheseScalars) -> pd.DataFrame:
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


def couverture_mrm(d: SyntheseScalars) -> pd.DataFrame:
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


def conformite_globale(d: SyntheseScalars) -> pd.DataFrame:
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

def chute_par_consigne(d: SyntheseScalars) -> pd.DataFrame:
    """Taux de chute par consigne pertinente (= graphe 4)."""
    df = consignes(d)
    return df[df["PM_PERTINENTE"]][
        ["CONSIGNE", "TAUX_CHUTE_PCT", "PM_MRM", "PM_CPT", "ECART"]
    ].reset_index(drop=True)


def pm_par_consigne(d: SyntheseScalars) -> pd.DataFrame:
    """PM revue MRM vs PM compte par consigne pertinente (= graphe 9)."""
    df = consignes(d)
    return df[df["PM_PERTINENTE"]][
        ["CONSIGNE", "PM_MRM", "PM_CPT", "ECART", "TAUX_CHUTE_PCT"]
    ].reset_index(drop=True)


def conformite_consignes(d: SyntheseScalars) -> pd.DataFrame:
    """Conformité par consigne, toutes consignes (= graphe 5)."""
    out = consignes(d)[
        ["CONSIGNE", "NB_TOTAL", "NB_CONFORMES", "PCT_CONFORMITE", "NB_KO", "NATURE_KO"]
    ].copy()
    out["PCT_KO"] = (100 - out["PCT_CONFORMITE"]).round(1)
    return out
