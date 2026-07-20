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
from core.metrics.scalaires import UNIVERS_COMPTE, UNIVERS_REVUE
from core.metrics.agregats import (
    AXE_ENSEMBLE, AXE_TYPE_COMPTE, AXE_ANCIENNETE, AXE_TRANCHE_ECART,
    AXE_GARANTIE, AXE_MOIS, AXE_CLAUSE,
)


# ============================================================================
# CONTRÔLES DE COHÉRENCE INTER-TABLES
# ============================================================================

def controles_coherence(tables: Dict[str, pd.DataFrame], d: SyntheseScalars) -> pd.DataFrame:
    """Recoupements INTER-TABLES : une même grandeur doit avoir la même valeur
    dans tous les onglets Power BI — l'étude raconte UNE histoire.

    Les tables issues de `d` se recoupent par construction ; les contrôles
    portent surtout sur les ré-agrégations Spark (blocs ventilés de `chute`
    et `orphelins`) et les sommes internes (consignes, bilan_cas, couverture).
    Une ligne par contrôle : attendu, obtenu, OK. Exportée avec les autres
    tables ; bloquante dans le run de production.
    """
    rows = []

    def ctrl(nom, attendu, obtenu, tol=0.0):
        ecart = (obtenu or 0) - (attendu or 0)
        rows.append({"CONTROLE": nom, "ATTENDU": attendu, "OBTENU": obtenu,
                     "ECART": round(ecart, 2), "OK": abs(ecart) <= tol})

    # chute : dans chaque bloc EXERCICE, chaque axe ventilé (ré-agrégation
    # Spark) recoupe la ligne « Ensemble » (scalaires de d) — Σ nb / PM.
    # L'axe « Tranche d'écart » (distribution des écarts) partitionne le même
    # univers : il recoupe comme les autres. Tolérance 1 € sur les PM :
    # arrondi à 2 décimales par ligne.
    ch = tables["chute"]
    refs = (
        (EXERCICE_INV, d["metrics_nb"],  d["metrics_pm_mrm"],  d["metrics_pm_cpt"]),
        (EXERCICE_N1,  d["chute_n1_nb"], d["chute_n1_pm_mrm"], d["chute_n1_pm_cpt"]),
    )
    for axe in (AXE_TYPE_COMPTE, AXE_ANCIENNETE, AXE_TRANCHE_ECART):
        for exercice, nb_ref, pm_mrm_ref, pm_cpt_ref in refs:
            bloc = ch[(ch["AXE"] == axe) & (ch["EXERCICE"] == exercice)]
            ctrl(f"chute {axe} / {exercice} : Σ nb = Ensemble",     nb_ref,     int(bloc["NB_DOSSIERS"].sum()))
            ctrl(f"chute {axe} / {exercice} : Σ PM MRM = Ensemble", pm_mrm_ref, float(bloc["PM_MRM"].sum()), tol=1.0)
            ctrl(f"chute {axe} / {exercice} : Σ PM CPT = Ensemble", pm_cpt_ref, float(bloc["PM_CPT"].sum()), tol=1.0)

    # orphelins : chaque axe PARTITIONNANT recoupe les CPT_ONLY → Σ nb =
    # def_nb (et Σ PM = def_pm sur deux axes témoins). L'axe « Clause
    # (détail) » est volontairement exclu de ce recoupement : il ne garde que
    # les lignes portant une clause (sous-ensemble), sa somme est donc
    # INFÉRIEURE au total — c'est attendu, pas une incohérence. Son garde-fou
    # est ci-dessous (il ne peut pas dépasser). L'axe « Composante de clé
    # nulle » n'est pas additif (un dossier compte plusieurs fois).
    orph = tables["orphelins"]
    for axe in (AXE_TYPE_COMPTE, AXE_GARANTIE, AXE_ANCIENNETE, AXE_MOIS):
        ctrl(f"orphelins {axe} : Σ nb = CPT_ONLY", d["def_nb"],
             int(orph.loc[orph["AXE"] == axe, "NB_DOSSIERS"].sum()))
    for axe in (AXE_TYPE_COMPTE, AXE_MOIS):
        ctrl(f"orphelins {axe} : Σ PM CPT = CPT_ONLY", d["def_pm"],
             float(orph.loc[orph["AXE"] == axe, "PM_CPT"].sum()), tol=1.0)
    nb_avec_clause = int(orph.loc[orph["AXE"] == AXE_CLAUSE, "NB_DOSSIERS"].sum())
    rows.append({
        "CONTROLE": f"orphelins {AXE_CLAUSE} : Σ nb ≤ CPT_ONLY (sous-ensemble porteur de clause)",
        "ATTENDU" : d["def_nb"], "OBTENU": nb_avec_clause,
        "ECART"   : nb_avec_clause - (d["def_nb"] or 0),
        "OK"      : nb_avec_clause <= (d["def_nb"] or 0),
    })

    # consignes : dans le bloc inventaire courant, Σ des bases PM pertinentes
    # (KAS + sans consigne reconnue) == base chute (cf. chute_coherente) ;
    # dans le bloc N+1, Σ des bases == base chute N+1 (DELETE exclu).
    cons = tables["consignes"]
    inv  = cons[(cons["EXERCICE"] == EXERCICE_INV) & cons["PM_PERTINENTE"].fillna(False)]
    ctrl("consignes inv. : Σ base (KAS + sans consigne) = base chute",
         d["metrics_nb"], int(inv["NB_BASE_CHUTE"].sum()))
    ctrl("consignes inv. : Σ PM MRM (KAS + sans consigne) = base chute",
         d["metrics_pm_mrm"], float(inv["PM_MRM"].sum()), tol=1.0)
    n1 = cons[cons["EXERCICE"] == EXERCICE_N1]
    ctrl("consignes N+1 : Σ base = base chute N+1",
         d["chute_n1_nb"], int(n1["NB_BASE_CHUTE"].sum()))

    # consignes_par_type_compte (ré-agrégation Spark) vs consignes (bloc
    # inventaire courant, issu de d) : mêmes règles de suivi, ventilées par
    # TYPE_COMPTE — Σ des types de compte d'une consigne = la ligne globale.
    cpc      = tables["consignes_par_type_compte"]
    cons_idx = cons[cons["EXERCICE"] == EXERCICE_INV].set_index("CONSIGNE")
    for code, libelle in (("KEEP", "À conserver"), ("ADD", "À ajouter"),
                          ("STUDY", "À étudier"), ("DELETE", "À supprimer")):
        if libelle not in cons_idx.index:
            continue
        sel = cpc[cpc["CONSIGNE"] == code]
        ctrl(f"consignes_par_type_compte {code} : Σ suivies = conformes",
             int(cons_idx.loc[libelle, "NB_CONFORMES"]), int(sel["NB_SUIVIES"].sum()))
        ctrl(f"consignes_par_type_compte {code} : Σ non suivies = KO",
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

    # couverture : chaque univers boucle sur son total.
    cov    = tables["couverture"]
    compte = cov[cov["UNIVERS"] == UNIVERS_COMPTE]
    ctrl("couverture Compte : Σ nb = compte entier",
         d["cpt_nb"], int(compte["NB_DOSSIERS"].sum()))
    revue  = cov[cov["UNIVERS"] == UNIVERS_REVUE]
    ctrl("couverture Revue MRM : retrouvés + non retrouvés = à comparer",
         d["a_comparer_nb"],
         int(revue.loc[~revue["CATEGORIE"].str.contains("supprimer"), "NB_DOSSIERS"].sum()))

    # synthese ↔ chute : la ligne « Ensemble » (inventaire) == les KPI de tête.
    ens_inv = ch[(ch["AXE"] == AXE_ENSEMBLE) & (ch["EXERCICE"] == EXERCICE_INV)]
    ctrl("chute Ensemble (inventaire) = synthese (base chute)",
         int(tables["synthese"]["NB_BASE_CHUTE"].iloc[0]),
         int(ens_inv["NB_DOSSIERS"].iloc[0]))

    return pd.DataFrame(rows)
