"""
Métriques scalaires — reshape du dict de compute_synthese en tables pandas.

Chaque fonction prend `d` (SyntheseScalars — LA passe Spark, déjà faite) et
renvoie un DataFrame pandas tidy en DONNÉES BRUTES (le formatage M€/% reste
au niveau restitution). Correspondance graphiques : cf. core/metrics/__init__.py.

Les tables exportées regroupent chacune UN sujet complet (contrat :
docs/METRIQUES.md §6) : `consignes` porte les deux exercices (colonne
EXERCICE), `couverture` les deux univers (colonne UNIVERS).
"""

import pandas as pd

from config import RUN_PARAMS
from core.synthese.synthese_contract import SyntheseScalars
from core.metrics.base import EXERCICE_INV, EXERCICE_N1, _annee_inventaire

# Libellés de lignes/blocs des tables regroupées — un seul vocabulaire.
SANS_CONSIGNE  = "Sans consigne reconnue"   # matché, MRM_ACTION nulle/inconnue
UNIVERS_COMPTE = "Compte"                   # couverture : le compte est-il justifié ?
UNIVERS_REVUE  = "Revue MRM"                # couverture : la revue est-elle retrouvée ?


# ============================================================================
# MÉTRIQUES SCALAIRES (reshape du dict de compute_synthese)
# ============================================================================

def dim_run(d: SyntheseScalars) -> pd.DataFrame:
    """La dimension de run — une ligne par run, pivot du modèle en étoile Power BI.

    Les colonnes de run du schéma standard (CLE_RUN, DATE_INVENTAIRE,
    PERIMETRE, LIBELLE_RUN) sont posées par export_metriques sur TOUTES les
    tables, celle-ci comprise ; cette table y ajoute les attributs qui ne
    vivent qu'au niveau du run : année d'inventaire, vision comptable CPT,
    présence d'une récupération N+1 (explique un bloc « Récupérés N+1 » vide).
    Reliée en 1-n à chaque table métrique par CLE_RUN : un seul segment
    (date d'inventaire / périmètre) pilote tout le rapport.
    """
    return pd.DataFrame([{
        "ANNEE_INVENTAIRE": _annee_inventaire(d),
        "VISION_CPT"      : RUN_PARAMS.get("cpt_vision"),
        "AVEC_MRM_N1"     : bool(RUN_PARAMS.get("fichier_mrm_n1")),
    }])

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
         "hors « à supprimer » N+1 — taux de chute et consignes propres "
         "(bloc N+1 des tables chute et consignes)"),
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


def consignes(d: SyntheseScalars) -> pd.DataFrame:
    """LE suivi des consignes — les deux exercices dans une table (graphes 4, 5, 8, 9).

    Une ligne par consigne × exercice, la colonne EXERCICE sépare les univers
    (même règle que la table `chute`, cf. METRIQUES §4.2) :
    - « Inventaire courant » : conformité + PM + taux de chute par consigne
      (exercice courant pur, §5), plus la ligne « Sans consigne reconnue » —
      dossiers matchés sans consigne exploitable : pas de conformité à mesurer,
      mais leur PM est DANS la base chute (§4.3) ;
    - « Récupérés N+1 » : suivi des consignes N+1 (analyse séparée, hors stats
      globales) — volumétries seules, les PM du bloc N+1 sont dans `chute`.

    Le KO reste nommé par le fait (« non retrouvé » / « encore au compte ») ;
    les graphes par consigne se lisent en filtrant EXERCICE = inventaire courant.
    """
    rows = []
    for consigne, c in d["consignes"].items():
        rows.append({
            "EXERCICE"        : EXERCICE_INV,
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
    hc_mrm, hc_cpt = d["hors_consigne_pm_mrm"], d["hors_consigne_pm_cpt"]
    rows.append({
        "EXERCICE"        : EXERCICE_INV,
        "CONSIGNE"        : SANS_CONSIGNE,
        "NB_TOTAL"        : None,                  # conformité sans objet
        "NB_CONFORMES"    : None,
        "PCT_CONFORMITE"  : None,
        "NB_KO"           : None,
        "NATURE_KO"       : None,
        "NB_BASE_CHUTE"   : d["hors_consigne_nb"],
        "PM_MRM"          : hc_mrm,
        "PM_CPT"          : hc_cpt,
        "ECART"           : hc_mrm - hc_cpt,
        "TAUX_CHUTE_PCT"  : round((hc_mrm - hc_cpt) / hc_mrm * 100, 2) if hc_mrm else 0.0,
        "PM_PERTINENTE"   : True,
    })
    # Bloc N+1 : présent seulement si le run a une récupération N+1. Un récupéré
    # est retrouvé PAR CONSTRUCTION → conforme, sauf « à supprimer » (encore au
    # compte). « À supprimer » N+1 est hors base chute N+1 (NB_BASE_CHUTE nul).
    for consigne, nb in d["n1_consignes"].items():
        delete = consigne == "À supprimer"
        rows.append({
            "EXERCICE"        : EXERCICE_N1,
            "CONSIGNE"        : consigne,
            "NB_TOTAL"        : nb,
            "NB_CONFORMES"    : 0 if delete else nb,
            "PCT_CONFORMITE"  : (0.0 if delete else 100.0) if nb else None,
            "NB_KO"           : nb if delete else 0,
            "NATURE_KO"       : "encore au compte" if delete else None,
            "NB_BASE_CHUTE"   : None if delete else nb,
            "PM_MRM"          : None,
            "PM_CPT"          : None,
            "ECART"           : None,
            "TAUX_CHUTE_PCT"  : None,
            "PM_PERTINENTE"   : not delete,
        })
    if d["n1_consignes"] or d["n1_sans_consigne"]:
        rows.append({
            "EXERCICE"        : EXERCICE_N1,
            "CONSIGNE"        : SANS_CONSIGNE,
            "NB_TOTAL"        : None,
            "NB_CONFORMES"    : None,
            "PCT_CONFORMITE"  : None,
            "NB_KO"           : None,
            "NATURE_KO"       : None,
            "NB_BASE_CHUTE"   : d["n1_sans_consigne"],
            "PM_MRM"          : None,
            "PM_CPT"          : None,
            "ECART"           : None,
            "TAUX_CHUTE_PCT"  : None,
            "PM_PERTINENTE"   : True,
        })
    return pd.DataFrame(rows)


def couverture(d: SyntheseScalars) -> pd.DataFrame:
    """LA couverture — les deux univers dans une table (graphes 1 et 2).

    Une ligne par catégorie × univers, la colonne UNIVERS sépare les lectures :
    - « Compte » : le compte est-il justifié ? décomposition en retrouvés,
      récupérés N+1, repêchés statut NON, clos avant inventaire, anomalies —
      PM côté compte, poids en nombre ET en PM ;
    - « Revue MRM » : la revue est-elle retrouvée ? part retrouvée au compte +
      non retrouvés par consigne (+ « à supprimer » encore au compte) — PM
      côté revue, poids en nombre.

    PCT_NB = poids dans son univers (« à supprimer » : part de sa propre
    consigne) ; PCT_PM = poids en PM (univers compte seul).
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
    rows = [{
        "UNIVERS"     : UNIVERS_COMPTE,
        "CATEGORIE"   : lbl,
        "NB_DOSSIERS" : nb,
        "PM_MRM"      : None,
        "PM_CPT"      : pm,
        "PCT_NB"      : round(nb / tot_nb * 100, 1),
        "PCT_PM"      : round(pm / tot_pm * 100, 1),
    } for lbl, nb, pm in cats]

    base   = d["a_comparer_nb"] or 1
    c_del  = d["consignes"]["À supprimer"]
    del_ko = c_del["nb"] - c_del["conf"]
    revue = [
        ("Retrouvés au compte",                  d["match_nb"], round(d["match_nb"] / base * 100, 1), None),
        ("À conserver non retrouvé",             d["keep_nb"],  round(d["keep_nb"]  / base * 100, 1), d["keep_pm"]),
        ("À étudier non retrouvé",               d["study_nb"], round(d["study_nb"] / base * 100, 1), d["study_pm"]),
        ("À ajouter non retrouvé",               d["add_nb"],   round(d["add_nb"]   / base * 100, 1), d["add_pm"]),
        ("« À supprimer » retrouvées au compte", del_ko,        round(del_ko / (c_del["nb"] or 1) * 100, 1), c_del["pm_mrm"]),
    ]
    rows += [{
        "UNIVERS"     : UNIVERS_REVUE,
        "CATEGORIE"   : lbl,
        "NB_DOSSIERS" : nb,
        "PM_MRM"      : pm,
        "PM_CPT"      : None,
        "PCT_NB"      : pct,
        "PCT_PM"      : None,
    } for lbl, nb, pct, pm in revue]
    return pd.DataFrame(rows)
