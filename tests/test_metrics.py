"""Tests de la couche métriques — logique pure (pandas), sans Spark.

Couvre les finalisations par axe, les tables regroupées (consignes,
couverture, assemblage de chute / orphelins) et les recoupements
inter-tables de bout en bout sur un `d` réaliste (_scalars_from_rows).
"""

import pytest

pytest.importorskip("pyspark")          # core.metrics importe pyspark au chargement
pd = pytest.importorskip("pandas")

from core.synthese.synthese_scalars import _scalars_from_rows
from core.metrics import (
    _finalise_chute_par_type_compte, _finalise_chute_par_anciennete,
    _finalise_consignes_par_type_compte,
    _finalise_orphelins, _annee_inventaire, EXERCICE_INV, EXERCICE_N1,
    BLOC_N, BLOC_N1, BLOC_N2_PLUS,
    consignes, couverture, synthese, bilan_cas,
    _assemble_chute, _assemble_orphelins,
    AXE_ENSEMBLE, AXE_TYPE_COMPTE, AXE_ANCIENNETE,
    AXE_GARANTIE, AXE_MOIS, AXE_CLAUSE, AXE_CLE_NULLE,
    SANS_CONSIGNE, UNIVERS_COMPTE, UNIVERS_REVUE,
)
from core.metrics.agregats import _ensemble_chute
from core.metrics.coherence import controles_coherence


def _ligne(type_compte, pm_mrm, pm_cpt, ecart):
    return {
        "EXERCICE": EXERCICE_INV, "TYPE_COMPTE": type_compte,
        "NB_DOSSIERS": 10, "NB_SOUS_PROVISION": 1, "NB_SUR_PROVISION": 0,
        "NB_ECART_NUL": 9,
        "PM_MRM": pm_mrm, "PM_CPT": pm_cpt, "ECART": ecart,
    }


def _ligne_anc(bloc, exercice, pm_mrm, pm_cpt, ecart):
    return {
        "EXERCICE": exercice, "BLOC_ANCIENNETE": bloc,
        "NB_DOSSIERS": 5, "NB_SOUS_PROVISION": 1, "NB_SUR_PROVISION": 0,
        "NB_ECART_NUL": 4,
        "PM_MRM": pm_mrm, "PM_CPT": pm_cpt, "ECART": ecart,
    }


def test_taux_chute_et_poids_pm_par_exercice():
    pdf = pd.DataFrame([
        _ligne("PB",  100.0, 80.0, 20.0),
        _ligne("HPB", 100.0, 50.0, 50.0),
    ])
    out = _finalise_chute_par_type_compte(pdf)
    row_pb = out[out["TYPE_COMPTE"] == "PB"].iloc[0]
    assert row_pb["TAUX_CHUTE_PCT"] == 20.0        # 20 / 100 * 100
    assert row_pb["POIDS_PM_PCT"] == 50.0          # 100 / (100+100) * 100


def test_pm_mrm_nulle_donne_taux_zero():
    pdf = pd.DataFrame([_ligne("PB", 0.0, 0.0, 0.0)])
    out = _finalise_chute_par_type_compte(pdf)
    assert out.iloc[0]["TAUX_CHUTE_PCT"] == 0.0    # pas de division par zéro


def test_tri_par_pm_mrm_decroissante():
    pdf = pd.DataFrame([
        _ligne("HPB", 200.0, 0.0, 200.0),
        _ligne("PB",  300.0, 0.0, 300.0),
    ])
    out = _finalise_chute_par_type_compte(pdf)
    assert list(out["TYPE_COMPTE"]) == ["PB", "HPB"]


# ── chute_par_anciennete : ordre des blocs + taux/poids ──────────────────────

def test_anciennete_ordre_blocs_et_taux():
    # blocs volontairement désordonnés en entrée → tri N → N-1 → N-2+
    pdf = pd.DataFrame([
        _ligne_anc(BLOC_N2_PLUS, EXERCICE_INV, 100.0, 100.0, 0.0),
        _ligne_anc(BLOC_N,       EXERCICE_INV, 100.0, 80.0, 20.0),
        _ligne_anc(BLOC_N1,      EXERCICE_INV, 100.0, 50.0, 50.0),
    ])
    out = _finalise_chute_par_anciennete(pdf)
    assert list(out["BLOC_ANCIENNETE"]) == [BLOC_N, BLOC_N1, BLOC_N2_PLUS]
    rowN = out[out["BLOC_ANCIENNETE"] == BLOC_N].iloc[0]
    assert rowN["TAUX_CHUTE_PCT"] == 20.0          # 20 / 100 * 100
    assert rowN["POIDS_PM_PCT"] == round(100 / 300 * 100, 2)


def test_anciennete_poids_par_exercice():
    # poids calculé DANS l'exercice : N+1 isolé de l'inventaire courant
    pdf = pd.DataFrame([
        _ligne_anc(BLOC_N,  EXERCICE_INV, 100.0, 90.0, 10.0),
        _ligne_anc(BLOC_N1, EXERCICE_INV, 300.0, 270.0, 30.0),
        _ligne_anc(BLOC_N,  EXERCICE_N1,  50.0, 40.0, 10.0),
    ])
    out = _finalise_chute_par_anciennete(pdf)
    n1_row = out[(out["EXERCICE"] == EXERCICE_N1)].iloc[0]
    assert n1_row["POIDS_PM_PCT"] == 100.0         # seule ligne de son exercice


# ── _finalise_orphelins : poids + RANG ───────────────────────────────────────

def test_orphelins_poids_et_rang():
    pdf = pd.DataFrame([
        {"CLAUSE": "A", "TYPE_COMPTE": "PB", "NB_DOSSIERS": 30, "PM_CPT": 300.0},
        {"CLAUSE": "B", "TYPE_COMPTE": "PB", "NB_DOSSIERS": 70, "PM_CPT": 700.0},
    ])
    out = _finalise_orphelins(pdf, with_rang=True)
    # RANG 1 = plus gros volume (B), poids nb = 70 %
    rang1 = out[out["RANG"] == 1].iloc[0]
    assert rang1["CLAUSE"] == "B"
    assert rang1["POIDS_NB_PCT"] == 70.0
    assert rang1["POIDS_PM_PCT"] == 70.0


def test_orphelins_volume_nul_pas_de_division_par_zero():
    pdf = pd.DataFrame([{"CLAUSE": "X", "TYPE_COMPTE": "PB",
                         "NB_DOSSIERS": 0, "PM_CPT": 0.0}])
    out = _finalise_orphelins(pdf)
    assert out.iloc[0]["POIDS_NB_PCT"] == 0.0
    assert out.iloc[0]["POIDS_PM_PCT"] == 0.0


def test_orphelins_poids_sur_totaux_explicites():
    # Table de détail (orphelins_par_clause) : les lignes ne couvrent qu'une
    # PARTIE des orphelins — les poids doivent se lire en part du TOTAL fourni,
    # pas du sous-ensemble, sinon le détail afficherait 100 %.
    pdf = pd.DataFrame([{"CLAUSE": "A", "TYPE_COMPTE": "PB",
                         "NB_DOSSIERS": 25, "PM_CPT": 250.0}])
    out = _finalise_orphelins(pdf, tot_nb=100, tot_pm=1000.0)
    assert out.iloc[0]["POIDS_NB_PCT"] == 25.0     # 25 / 100, pas 25 / 25
    assert out.iloc[0]["POIDS_PM_PCT"] == 25.0


# ── _annee_inventaire : parse de d["date_inventaire"] ────────────────────────

def test_annee_inventaire_parse():
    assert _annee_inventaire({"date_inventaire": "30/06/2024"}) == 2024
    assert _annee_inventaire({"date_inventaire": "auto"}) is None
    assert _annee_inventaire({"date_inventaire": "n/d"}) is None
    assert _annee_inventaire({}) is None


# ── _finalise_consignes_par_type_compte : tableau de bord des consignes ──────

def test_finalise_consignes_par_type_compte():
    pdf = pd.DataFrame([
        {"TYPE_COMPTE": "PB", "MRM_ACTION": "MRM_DELETE",
         "NB_DOSSIERS": 10, "NB_SUIVIES": 8, "NB_NON_SUIVIES": 2,
         "PM_MRM": 100.456, "PM_CPT": 90.0, "NB_NON_REMONTE_DF": 3},
        {"TYPE_COMPTE": "PB", "MRM_ACTION": "MRM_KEEP",
         "NB_DOSSIERS": 40, "NB_SUIVIES": 30, "NB_NON_SUIVIES": 10,
         "PM_MRM": 400.0, "PM_CPT": 380.0, "NB_NON_REMONTE_DF": 0},
    ])
    out = _finalise_consignes_par_type_compte(pdf)
    # Libellé sans préfixe MRM_ + ordre KEEP avant DELETE.
    assert list(out["CONSIGNE"]) == ["KEEP", "DELETE"]
    keep = out.iloc[0]
    assert keep["PCT_SUIVI"] == 75.0                    # 30 / 40
    assert out.iloc[1]["PM_MRM"] == 100.46              # arrondi 2 déc.
    assert "MRM_ACTION" not in out.columns              # colonne technique retirée
    assert "CLAUSE" not in out.columns                  # la clause n'est pas un axe


def test_finalise_consignes_par_type_compte_suivi_sans_dossier():
    pdf = pd.DataFrame([
        # Que des repêchés statut NON : suivi vide → pas de division par zéro.
        {"TYPE_COMPTE": "PB", "MRM_ACTION": "MRM_ADD",
         "NB_DOSSIERS": 0, "NB_SUIVIES": 0, "NB_NON_SUIVIES": 0,
         "PM_MRM": 0.0, "PM_CPT": 0.0, "NB_NON_REMONTE_DF": 5},
    ])
    out = _finalise_consignes_par_type_compte(pdf)
    assert out.iloc[0]["PCT_SUIVI"] == 0.0
    assert out.iloc[0]["NB_NON_REMONTE_DF"] == 5


# ============================================================================
# TABLES REGROUPÉES — un d réaliste via _scalars_from_rows (pure python)
# ============================================================================

def _row(type_rec, action=None, source=None, non=False,
         nb=1, pm_mrm=0.0, pm_cpt=0.0, nz_mrm=0, nz_cpt=0):
    return {
        "TYPE_RECONCILIATION": type_rec,
        "MRM_ACTION": action,
        "LATE_SOURCE": source,
        "IS_STATUT_NON": non,
        "nb": nb, "pm_mrm": pm_mrm, "pm_cpt": pm_cpt,
        "nb_pm_mrm_nz": nz_mrm, "nb_pm_cpt_nz": nz_cpt,
    }


def _d():
    """Le même mini-scénario que test_scalars_from_rows : chaque grande catégorie."""
    return _scalars_from_rows([
        _row("MATCH_EXACT", "MRM_KEEP",   pm_mrm=100.0, pm_cpt=80.0, nz_mrm=1, nz_cpt=1),
        _row("MRM_MISSING", "MRM_KEEP",   pm_mrm=50.0,  nz_mrm=1),
        _row("CPT_ONLY",    None,         pm_cpt=30.0,  nz_cpt=1),
        _row("MRM_DELETE",  "MRM_DELETE", pm_mrm=10.0,  nz_mrm=1),
        _row("CPT_LATE",    "MRM_KEEP",   source="MRM_N1",         pm_mrm=60.0, pm_cpt=55.0, nz_mrm=1, nz_cpt=1),
        _row("CPT_OBS_TARDIVE", None,     source="OBS_TARDIVE_IT", pm_cpt=20.0, nz_cpt=1),
        _row("CPT_RECUP_NON",   None,     source="STATUT_NON", non=True, pm_cpt=15.0, nz_cpt=1),
    ], "31/12/2023")


def test_consignes_regroupe_les_deux_exercices():
    out = consignes(_d())
    inv = out[out["EXERCICE"] == EXERCICE_INV]
    n1  = out[out["EXERCICE"] == EXERCICE_N1]
    # Inventaire : 4 consignes + la ligne « Sans consigne reconnue ».
    assert len(inv) == 5 and SANS_CONSIGNE in set(inv["CONSIGNE"])
    keep = inv[inv["CONSIGNE"] == "À conserver"].iloc[0]
    assert keep["NB_CONFORMES"] == 1 and keep["NB_KO"] == 1
    # N+1 : un récupéré est retrouvé par construction → conforme (sauf DELETE).
    keep_n1 = n1[n1["CONSIGNE"] == "À conserver"].iloc[0]
    assert keep_n1["NB_CONFORMES"] == 1 and keep_n1["PCT_CONFORMITE"] == 100.0
    assert keep_n1["NB_BASE_CHUTE"] == 1          # dans la base chute N+1
    # Σ bases pertinentes inventaire = base chute (règle §4.2).
    pert = inv[inv["PM_PERTINENTE"].fillna(False)]
    assert pert["NB_BASE_CHUTE"].sum() == _d()["metrics_nb"]


def test_couverture_regroupe_les_deux_univers():
    d = _d()
    out = couverture(d)
    compte = out[out["UNIVERS"] == UNIVERS_COMPTE]
    revue  = out[out["UNIVERS"] == UNIVERS_REVUE]
    # L'univers Compte boucle sur le compte entier ; PM côté compte seulement.
    assert compte["NB_DOSSIERS"].sum() == d["cpt_nb"]
    assert compte["PM_MRM"].isna().all() and revue["PM_CPT"].isna().all()
    # Revue : retrouvés + non retrouvés (hors « à supprimer ») = à comparer.
    hors_del = revue[~revue["CATEGORIE"].str.contains("supprimer")]
    assert hors_del["NB_DOSSIERS"].sum() == d["a_comparer_nb"]


def _chute_fixture(d):
    """La table chute assemblée depuis des ventilations cohérentes avec d."""
    tc = _finalise_chute_par_type_compte(pd.DataFrame([
        {"EXERCICE": EXERCICE_INV, "TYPE_COMPTE": "PB", "NB_DOSSIERS": 1,
         "NB_SOUS_PROVISION": 1, "NB_SUR_PROVISION": 0, "NB_ECART_NUL": 0,
         "PM_MRM": 100.0, "PM_CPT": 80.0, "ECART": 20.0},
        {"EXERCICE": EXERCICE_N1, "TYPE_COMPTE": "PB", "NB_DOSSIERS": 1,
         "NB_SOUS_PROVISION": 1, "NB_SUR_PROVISION": 0, "NB_ECART_NUL": 0,
         "PM_MRM": 60.0, "PM_CPT": 55.0, "ECART": 5.0},
    ]))
    anc = _finalise_chute_par_anciennete(pd.DataFrame([
        {"EXERCICE": EXERCICE_INV, "BLOC_ANCIENNETE": BLOC_N, "NB_DOSSIERS": 1,
         "NB_SOUS_PROVISION": 1, "NB_SUR_PROVISION": 0, "NB_ECART_NUL": 0,
         "PM_MRM": 100.0, "PM_CPT": 80.0, "ECART": 20.0},
        {"EXERCICE": EXERCICE_N1, "BLOC_ANCIENNETE": BLOC_N, "NB_DOSSIERS": 1,
         "NB_SOUS_PROVISION": 1, "NB_SUR_PROVISION": 0, "NB_ECART_NUL": 0,
         "PM_MRM": 60.0, "PM_CPT": 55.0, "ECART": 5.0},
    ]))
    return _assemble_chute(_ensemble_chute(d), tc, anc)


def test_assemble_chute_trois_axes_et_taux_officiels():
    d   = _d()
    out = _chute_fixture(d)
    assert list(out["AXE"].unique()) == [AXE_ENSEMBLE, AXE_TYPE_COMPTE, AXE_ANCIENNETE]
    ens_inv = out[(out["AXE"] == AXE_ENSEMBLE) & (out["EXERCICE"] == EXERCICE_INV)].iloc[0]
    # La ligne « Ensemble » porte le taux OFFICIEL de compute_synthese.
    assert ens_inv["TAUX_CHUTE_PCT"] == d["taux_chute_inventaire"]
    assert ens_inv["SEGMENT"] == AXE_ENSEMBLE and ens_inv["POIDS_PM_PCT"] == 100.0


def _orphelins_fixture():
    """La table orphelins assemblée depuis les six angles (1 orphelin de 30 €)."""
    tc = _finalise_orphelins(pd.DataFrame(
        [{"TYPE_COMPTE": "PB", "NB_DOSSIERS": 1, "PM_CPT": 30.0}]), with_rang=True)
    gar = _finalise_orphelins(pd.DataFrame(
        [{"GARANTIE_CODE": 60, "GARANTIE_LIBELLE": "IT (incapacité)",
          "NB_DOSSIERS": 1, "PM_CPT": 30.0}]))
    anc = _finalise_orphelins(pd.DataFrame(
        [{"BLOC_ANCIENNETE": BLOC_N, "NB_DOSSIERS": 1, "PM_CPT": 30.0}]))
    mois = pd.DataFrame([{"MOIS_SURVENANCE": 12, "MOIS_LABEL": "Déc",
                          "NB_DOSSIERS": 1, "PM_CPT": 30.0, "IS_FIN_ANNEE": True}])
    cla = _finalise_orphelins(pd.DataFrame(
        [{"CLAUSE": "C1", "TYPE_COMPTE": "PB", "NB_DOSSIERS": 1, "PM_CPT": 30.0}]),
        with_rang=True)
    cle = pd.DataFrame([{"COMPOSANTE": "CPT_RPP", "NB_NULL_OU_VIDE": 1,
                         "PCT_NULL": 100.0, "NB_TOTAL_ORPHELINS": 1}])
    return _assemble_orphelins(tc, gar, anc, mois, cla, cle)


def test_assemble_orphelins_six_angles():
    out  = _orphelins_fixture()
    axes = list(out["AXE"].unique())
    assert axes == [AXE_TYPE_COMPTE, AXE_GARANTIE, AXE_ANCIENNETE,
                    AXE_MOIS, AXE_CLAUSE, AXE_CLE_NULLE]
    # Mois : SEGMENT = libellé, ORDRE = numéro du mois, poids recalculés.
    mois = out[out["AXE"] == AXE_MOIS].iloc[0]
    assert mois["SEGMENT"] == "Déc" and mois["ORDRE"] == 12
    assert mois["POIDS_NB_PCT"] == 100.0
    # Composante de clé : nb = nullité, poids = % de nullité, pas de PM.
    cle = out[out["AXE"] == AXE_CLE_NULLE].iloc[0]
    assert cle["NB_DOSSIERS"] == 1 and cle["POIDS_NB_PCT"] == 100.0
    assert pd.isna(cle["PM_CPT"])
    # Clause : le TYPE_COMPTE fait partie du grain, il est conservé.
    assert out[out["AXE"] == AXE_CLAUSE].iloc[0]["TYPE_COMPTE"] == "PB"


def test_controles_coherence_de_bout_en_bout():
    """Les 8 tables construites sur le même scénario → tous les recoupements OK."""
    d   = _d()
    cpc = pd.DataFrame([
        {"TYPE_COMPTE": "PB", "CONSIGNE": "KEEP", "NB_DOSSIERS": 2,
         "NB_SUIVIES": 1, "NB_NON_SUIVIES": 1, "PCT_SUIVI": 50.0,
         "PM_MRM": 150.0, "PM_CPT": 80.0, "NB_NON_REMONTE_DF": 0},
        {"TYPE_COMPTE": "PB", "CONSIGNE": "DELETE", "NB_DOSSIERS": 1,
         "NB_SUIVIES": 1, "NB_NON_SUIVIES": 0, "PCT_SUIVI": 100.0,
         "PM_MRM": 10.0, "PM_CPT": 0.0, "NB_NON_REMONTE_DF": 0},
    ])
    tables = {
        "synthese"                 : synthese(d),
        "bilan_cas"                : bilan_cas(d),
        "couverture"               : couverture(d),
        "chute"                    : _chute_fixture(d),
        "consignes"                : consignes(d),
        "consignes_par_type_compte": cpc,
        "orphelins"                : _orphelins_fixture(),
    }
    ctrl = controles_coherence(tables, d)
    ko = ctrl[~ctrl["OK"]]
    assert ko.empty, f"contrôles KO :\n{ko[['CONTROLE', 'ATTENDU', 'OBTENU']]}"
