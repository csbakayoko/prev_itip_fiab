"""Tests de l'export Excel — logique pure (pandas/openpyxl, sans Spark)."""

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("openpyxl")

from core.io.excel_export import _safe_sheet_name, _number_format


def test_safe_sheet_name_garde_les_noms_valides():
    assert _safe_sheet_name("Couverture revue MRM", set()) == "Couverture revue MRM"


def test_safe_sheet_name_retire_les_caracteres_interdits():
    name = _safe_sheet_name("a:b/c?d*e[f]", set())
    assert not (set(name) & set(r":\/?*[]"))


def test_safe_sheet_name_tronque_a_31():
    assert len(_safe_sheet_name("x" * 40, set())) <= 31


def test_safe_sheet_name_unicite():
    used = set()
    a = _safe_sheet_name("Onglet", used)
    b = _safe_sheet_name("Onglet", used)
    assert a != b and a == "Onglet"


def test_number_format_pourcentage():
    assert _number_format("TAUX_CHUTE_PCT", pd.Series([45.3, 10.0])) == '0.0"%"'
    assert _number_format("POIDS_PM_PCT", pd.Series([1.0, 2.0])) == '0.0"%"'


def test_number_format_euro():
    assert _number_format("PM_MRM", pd.Series([1000.0, 2000.0])) == '#,##0 €'
    assert _number_format("ECART", pd.Series([-5.0, 5.0])) == '#,##0 €'


def test_number_format_entier():
    assert _number_format("NB_DOSSIERS", pd.Series([1, 2, 3])) == '#,##0'


def test_number_format_bool_et_texte_ignores():
    assert _number_format("PM_PERTINENTE", pd.Series([True, False])) is None
    assert _number_format("CONSIGNE", pd.Series(["a", "b"])) is None
