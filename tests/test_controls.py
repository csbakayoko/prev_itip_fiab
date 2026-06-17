"""Tests des contrôles qualité — détection des colonnes au nom invalide."""

import pytest

pytest.importorskip("pyspark")          # core.prep.controls importe pyspark au chargement

from core.prep.controls import colonnes_nom_invalide


def test_detecte_noms_nuls_vides_et_auto_generes():
    cols = ["RPP", "", "  ", None, "_c0", "_c12", "PM"]
    bad = colonnes_nom_invalide(cols)
    assert "" in bad
    assert "  " in bad
    assert None in bad
    assert "_c0" in bad
    assert "_c12" in bad


def test_garde_les_noms_valides():
    bad = colonnes_nom_invalide(["RPP", "PM", "GARANTIE", "PM_EXO_INV"])
    assert bad == []
