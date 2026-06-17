"""Tests de la finalisation pandas de chute_par_clause (logique pure, sans Spark)."""

import pytest

pytest.importorskip("pyspark")          # core.metrics importe pyspark au chargement
pd = pytest.importorskip("pandas")

from core.metrics import _finalise_chute_par_clause, EXERCICE_INV


def _ligne(clause, pm_mrm, pm_cpt, ecart):
    return {
        "EXERCICE": EXERCICE_INV, "CLAUSE": clause, "TYPE_CLAUSE": "PB",
        "nb_dossiers": 10, "nb_sous": 1, "nb_sur": 0, "nb_conforme": 9,
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
