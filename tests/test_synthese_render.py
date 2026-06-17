"""Test du rendu ASCII de la synthèse — PUR (sans Spark). La dérivation pure
alimente le rendu pur : on vérifie en local que render_synthese produit bien une
synthèse lisible (bulles, indicateurs, consignes, entête)."""

from core.synthese_scalars import _scalars_from_rows
from core.synthese_render import render_synthese


def _minimal_d():
    rows = [{
        "TYPE_RECONCILIATION": "MATCH_EXACT", "MRM_ACTION": "MRM_KEEP",
        "LATE_SOURCE": None, "IS_STATUT_NON": False,
        "nb": 1, "pm_mrm": 100.0, "pm_cpt": 80.0,
        "nb_pm_mrm_nz": 1, "nb_pm_cpt_nz": 1,
    }]
    return _scalars_from_rows(rows, "31/12/2023")


def test_render_synthese_lisible():
    txt = render_synthese(_minimal_d(), client="TEST_CLIENT")
    # les 3 bulles, les blocs, l'entête
    for attendu in ("MRM", "RETROUVÉS", "COMPTE", "INDICATEURS",
                    "SUIVI DES CONSIGNES", "TEST_CLIENT", "31/12/2023"):
        assert attendu in txt, f"« {attendu} » absent du rendu"
    assert isinstance(txt, str) and len(txt) > 200
