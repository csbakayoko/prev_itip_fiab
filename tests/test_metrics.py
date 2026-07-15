"""Tests de la finalisation pandas de chute_par_clause (logique pure, sans Spark)."""

import pytest

pytest.importorskip("pyspark")          # core.metrics importe pyspark au chargement
pd = pytest.importorskip("pandas")

from core.metrics import (
    _finalise_chute_par_clause, _finalise_chute_par_anciennete,
    _finalise_consignes_par_clause,
    _finalise_orphelins, _annee_inventaire, EXERCICE_INV, EXERCICE_N1,
    BLOC_N, BLOC_N1, BLOC_N2_PLUS,
)


def _ligne(clause, pm_mrm, pm_cpt, ecart):
    return {
        "EXERCICE": EXERCICE_INV, "CLAUSE": clause, "TYPE_COMPTE": "PB",
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
        _ligne("A", 100.0, 80.0, 20.0),
        _ligne("B", 100.0, 50.0, 50.0),
    ])
    out = _finalise_chute_par_clause(pdf)
    rowA = out[out["CLAUSE"] == "A"].iloc[0]
    assert rowA["TAUX_CHUTE_PCT"] == 20.0          # 20 / 100 * 100
    assert rowA["POIDS_PM_PCT"] == 50.0            # 100 / (100+100) * 100


def test_pm_mrm_nulle_donne_taux_zero():
    pdf = pd.DataFrame([_ligne("C", 0.0, 0.0, 0.0)])
    out = _finalise_chute_par_clause(pdf)
    assert out.iloc[0]["TAUX_CHUTE_PCT"] == 0.0    # pas de division par zéro


def test_top_limite_par_exercice():
    pdf = pd.DataFrame([
        _ligne("A", 300.0, 0.0, 300.0),
        _ligne("B", 200.0, 0.0, 200.0),
        _ligne("C", 100.0, 0.0, 100.0),
    ])
    out = _finalise_chute_par_clause(pdf, top=2)
    assert len(out) == 2
    # trié par PM_MRM desc → garde A puis B
    assert list(out["CLAUSE"]) == ["A", "B"]


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


# ── _annee_inventaire : parse de d["date_inventaire"] ────────────────────────

def test_annee_inventaire_parse():
    assert _annee_inventaire({"date_inventaire": "30/06/2024"}) == 2024
    assert _annee_inventaire({"date_inventaire": "auto"}) is None
    assert _annee_inventaire({"date_inventaire": "n/d"}) is None
    assert _annee_inventaire({}) is None


# ── _finalise_consignes_par_clause : tableau de bord des consignes ───────────

def test_finalise_consignes_par_clause():
    pdf = pd.DataFrame([
        {"TYPE_COMPTE": "PB", "CLAUSE": "121981", "MRM_ACTION": "MRM_DELETE",
         "NB_DOSSIERS": 10, "NB_SUIVIES": 8, "NB_NON_SUIVIES": 2,
         "PM_MRM": 100.456, "PM_CPT": 90.0, "NB_NON_REMONTE_DF": 3},
        {"TYPE_COMPTE": "PB", "CLAUSE": "121981", "MRM_ACTION": "MRM_KEEP",
         "NB_DOSSIERS": 40, "NB_SUIVIES": 30, "NB_NON_SUIVIES": 10,
         "PM_MRM": 400.0, "PM_CPT": 380.0, "NB_NON_REMONTE_DF": 0},
    ])
    out = _finalise_consignes_par_clause(pdf)
    # Libellé sans préfixe MRM_ + ordre KEEP avant DELETE.
    assert list(out["CONSIGNE"]) == ["KEEP", "DELETE"]
    keep = out.iloc[0]
    assert keep["PCT_SUIVI"] == 75.0                    # 30 / 40
    assert out.iloc[1]["PM_MRM"] == 100.46              # arrondi 2 déc.
    assert "MRM_ACTION" not in out.columns              # colonne technique retirée


def test_finalise_consignes_par_clause_suivi_sans_dossier():
    pdf = pd.DataFrame([
        # Que des repêchés statut NON : suivi vide → pas de division par zéro.
        {"TYPE_COMPTE": "PB", "CLAUSE": "9", "MRM_ACTION": "MRM_ADD",
         "NB_DOSSIERS": 0, "NB_SUIVIES": 0, "NB_NON_SUIVIES": 0,
         "PM_MRM": 0.0, "PM_CPT": 0.0, "NB_NON_REMONTE_DF": 5},
    ])
    out = _finalise_consignes_par_clause(pdf)
    assert out.iloc[0]["PCT_SUIVI"] == 0.0
    assert out.iloc[0]["NB_NON_REMONTE_DF"] == 5
