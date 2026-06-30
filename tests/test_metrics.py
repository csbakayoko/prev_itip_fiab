"""Tests de la finalisation pandas de chute_par_clause (logique pure, sans Spark)."""

import pytest

pytest.importorskip("pyspark")          # core.metrics importe pyspark au chargement
pd = pytest.importorskip("pandas")

from core.metrics import (
    _finalise_chute_par_clause, _finalise_chute_par_anciennete,
    _finalise_orphelins, _annee_inventaire, EXERCICE_INV, EXERCICE_N1,
    BLOC_N, BLOC_N1, BLOC_N2_PLUS,
)


def _ligne(clause, pm_mrm, pm_cpt, ecart):
    return {
        "EXERCICE": EXERCICE_INV, "CLAUSE": clause, "TYPE_CLAUSE": "PB",
        "nb_dossiers": 10, "nb_sous": 1, "nb_sur": 0, "nb_conforme": 9,
        "pm_mrm": pm_mrm, "pm_cpt": pm_cpt, "ecart_signe": ecart,
    }


def _ligne_anc(bloc, exercice, pm_mrm, pm_cpt, ecart):
    return {
        "EXERCICE": exercice, "BLOC_ANCIENNETE": bloc,
        "nb_dossiers": 5, "nb_sous": 1, "nb_sur": 0, "nb_conforme": 4,
        "pm_mrm": pm_mrm, "pm_cpt": pm_cpt, "ecart_signe": ecart,
    }


def test_taux_chute_et_poids_pm_par_exercice():
    pdf = pd.DataFrame([
        _ligne("A", 100.0, 80.0, 20.0),
        _ligne("B", 100.0, 50.0, 50.0),
    ])
    out = _finalise_chute_par_clause(pdf)
    rowA = out[out["CLAUSE"] == "A"].iloc[0]
    assert rowA["taux_chute_pct"] == 20.0          # 20 / 100 * 100
    assert rowA["poids_pm_pct"] == 50.0            # 100 / (100+100) * 100


def test_pm_mrm_nulle_donne_taux_zero():
    pdf = pd.DataFrame([_ligne("C", 0.0, 0.0, 0.0)])
    out = _finalise_chute_par_clause(pdf)
    assert out.iloc[0]["taux_chute_pct"] == 0.0    # pas de division par zéro


def test_top_limite_par_exercice():
    pdf = pd.DataFrame([
        _ligne("A", 300.0, 0.0, 300.0),
        _ligne("B", 200.0, 0.0, 200.0),
        _ligne("C", 100.0, 0.0, 100.0),
    ])
    out = _finalise_chute_par_clause(pdf, top=2)
    assert len(out) == 2
    # trié par pm_mrm desc → garde A puis B
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
    assert rowN["taux_chute_pct"] == 20.0          # 20 / 100 * 100
    assert rowN["poids_pm_pct"] == round(100 / 300 * 100, 2)


def test_anciennete_poids_par_exercice():
    # poids calculé DANS l'exercice : N+1 isolé de l'inventaire courant
    pdf = pd.DataFrame([
        _ligne_anc(BLOC_N,  EXERCICE_INV, 100.0, 90.0, 10.0),
        _ligne_anc(BLOC_N1, EXERCICE_INV, 300.0, 270.0, 30.0),
        _ligne_anc(BLOC_N,  EXERCICE_N1,  50.0, 40.0, 10.0),
    ])
    out = _finalise_chute_par_anciennete(pdf)
    n1_row = out[(out["EXERCICE"] == EXERCICE_N1)].iloc[0]
    assert n1_row["poids_pm_pct"] == 100.0         # seule ligne de son exercice


# ── _finalise_orphelins : poids + RANG ───────────────────────────────────────

def test_orphelins_poids_et_rang():
    pdf = pd.DataFrame([
        {"CLAUSE": "A", "TYPE_CLAUSE": "PB", "NB_DOSSIERS": 30, "PM_CPT": 300.0},
        {"CLAUSE": "B", "TYPE_CLAUSE": "PB", "NB_DOSSIERS": 70, "PM_CPT": 700.0},
    ])
    out = _finalise_orphelins(pdf, with_rang=True)
    # RANG 1 = plus gros volume (B), poids nb = 70 %
    rang1 = out[out["RANG"] == 1].iloc[0]
    assert rang1["CLAUSE"] == "B"
    assert rang1["POIDS_NB_PCT"] == 70.0
    assert rang1["POIDS_PM_PCT"] == 70.0


def test_orphelins_volume_nul_pas_de_division_par_zero():
    pdf = pd.DataFrame([{"CLAUSE": "X", "TYPE_CLAUSE": "PB",
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
