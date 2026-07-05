"""
Contrôles de cohérence inter-tables — les onglets Power BI doivent se recouper.

Une même grandeur doit avoir la même valeur dans tous les onglets : chaque
recoupement produit une ligne ATTENDU / OBTENU / OK. Bloquant en production
(itip_fiab_powerbi) via la table controles_coherence.
"""

from typing import Dict

import pandas as pd

from core.synthese.synthese_contract import SyntheseScalars
from core.metrics.base import EXERCICE_INV, EXERCICE_N1


# ============================================================================
# CONTRÔLES DE COHÉRENCE INTER-TABLES
# ============================================================================

def controles_coherence(tables: Dict[str, pd.DataFrame], d: SyntheseScalars) -> pd.DataFrame:
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

    # chute_par_anciennete (ré-agrégation Spark) vs base chute / bloc N+1 de d :
    # même univers que chute_par_clause, découpé par année de survenance.
    anc     = tables["chute_par_anciennete"]
    anc_inv = anc[anc["EXERCICE"] == EXERCICE_INV]
    anc_n1  = anc[anc["EXERCICE"] == EXERCICE_N1]
    ctrl("chute_par_anciennete inv. : Σ nb = base chute",     d["metrics_nb"],     int(anc_inv["nb_dossiers"].sum()))
    ctrl("chute_par_anciennete inv. : Σ PM MRM = base chute", d["metrics_pm_mrm"], float(anc_inv["pm_mrm"].sum()), tol=1.0)
    ctrl("chute_par_anciennete inv. : Σ PM CPT = base chute", d["metrics_pm_cpt"], float(anc_inv["pm_cpt"].sum()), tol=1.0)
    ctrl("chute_par_anciennete N+1 : Σ nb = base chute N+1",  d["chute_n1_nb"],    int(anc_n1["nb_dossiers"].sum()))

    # anomalies_cpt_only (ré-agrégation Spark) vs CPT_ONLY de d.
    anom = tables["anomalies_cpt_only"]
    ctrl("anomalies : Σ nb = CPT_ONLY",     d["def_nb"], int(anom["NB_DOSSIERS"].sum()))
    ctrl("anomalies : Σ PM CPT = CPT_ONLY", d["def_pm"], float(anom["PM_CPT"].sum()), tol=1.0)

    # Investigation orphelins : chaque ventilation partitionne les CPT_ONLY →
    # Σ nb = def_nb (et Σ PM CPT = def_pm pour la clause).
    orph_cl = tables["orphelins_par_clause"]
    ctrl("orphelins_par_clause : Σ nb = CPT_ONLY",     d["def_nb"], int(orph_cl["NB_DOSSIERS"].sum()))
    ctrl("orphelins_par_clause : Σ PM CPT = CPT_ONLY", d["def_pm"], float(orph_cl["PM_CPT"].sum()), tol=1.0)
    ctrl("orphelins_par_garantie : Σ nb = CPT_ONLY",   d["def_nb"], int(tables["orphelins_par_garantie"]["NB_DOSSIERS"].sum()))
    ctrl("orphelins_par_anciennete : Σ nb = CPT_ONLY", d["def_nb"], int(tables["orphelins_par_anciennete"]["NB_DOSSIERS"].sum()))
    cles = tables["orphelins_cles_nulles"]
    ctrl("orphelins_cles_nulles : total orphelins = CPT_ONLY",
         d["def_nb"], int(cles["NB_TOTAL_ORPHELINS"].iloc[0]) if len(cles) else 0)

    # consignes : Σ bases KAS + hors consigne == base chute (cf. chute_coherente).
    kas = tables["consignes"][tables["consignes"]["PM_PERTINENTE"]]
    ctrl("consignes : Σ base KAS + hors consigne = base chute",
         d["metrics_nb"], int(kas["NB_BASE_CHUTE"].sum()) + d["hors_consigne_nb"])
    ctrl("consignes : Σ PM MRM KAS + hors consigne = base chute",
         d["metrics_pm_mrm"], float(kas["PM_MRM"].sum()) + d["hors_consigne_pm_mrm"], tol=1.0)

    # consignes_par_clause (ré-agrégation Spark) vs consignes (globale, issue
    # de d) : mêmes règles de suivi, ventilées par TYPE_COMPTE × CLAUSE —
    # Σ des clauses d'une consigne = la ligne globale de cette consigne.
    cpc      = tables["consignes_par_clause"]
    cons_idx = tables["consignes"].set_index("CONSIGNE")
    for code, libelle in (("KEEP", "À conserver"), ("ADD", "À ajouter"),
                          ("STUDY", "À étudier"), ("DELETE", "À supprimer")):
        if libelle not in cons_idx.index:
            continue
        sel = cpc[cpc["CONSIGNE"] == code]
        ctrl(f"consignes_par_clause {code} : Σ suivies = conformes",
             int(cons_idx.loc[libelle, "NB_CONFORMES"]), int(sel["NB_SUIVIES"].sum()))
        ctrl(f"consignes_par_clause {code} : Σ non suivies = KO",
             int(cons_idx.loc[libelle, "NB_KO"]), int(sel["NB_NON_SUIVIES"].sum()))

    # bilan_cas : totaux internes + recoupement avec la synthèse.
    b = tables["bilan_cas"].set_index("CAS")
    ctrl("bilan_cas : TOTAL matchés = Σ des 4 clés",
         int(b.loc["TOTAL matchés", "NB_DOSSIERS"]),
         int(b.loc["Clé principale (nom complet + dates)", "NB_DOSSIERS"]
             + b.loc["Clé affinée (nom tronqué 20 car.)", "NB_DOSSIERS"]
             + b.loc["Récupération (IT→IP, rechutes)", "NB_DOSSIERS"]
             + b.loc["Clé clause (RPP absent/non fiable)", "NB_DOSSIERS"]))
    ctrl("bilan_cas : base chute = synthese",
         int(tables["synthese"]["NB_BASE_CHUTE"].iloc[0]),
         int(b.loc["└ base du taux de chute", "NB_DOSSIERS"]))
    # Repêchés statut NON : le total recoupe la somme des deux exercices N + N+1.
    ctrl("bilan_cas : repêchés NON = N + N+1",
         int(b.loc["└ total repêchés statut NON", "NB_DOSSIERS"]),
         int(b.loc["Repêchés statut NON — exercice N", "NB_DOSSIERS"]
             + b.loc["Repêchés statut NON — exercice N+1", "NB_DOSSIERS"]))

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
