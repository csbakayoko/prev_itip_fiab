"""Tests des helpers scalaires de la synthèse (logique pure)."""

import pytest

pytest.importorskip("pyspark")          # core.synthese.kpi_export importe pyspark au chargement

from core.synthese.synthese_scalars import _pct   # _pct vit avec la dérivation pure (#1)
from core.synthese.kpi_export import kas_totaux


def test_pct_nominal_et_denominateur_nul():
    assert _pct(50, 200) == 25.0
    assert _pct(1, 0) == 0.0            # garde anti division par zéro


def test_kas_totaux_somme_conserver_etudier_ajouter():
    cons = {
        "À conserver" : {"nb": 2, "conf": 1, "pm_mrm": 10.0, "pm_cpt": 4.0, "delta": 6.0},
        "À étudier"   : {"nb": 1, "conf": 1, "pm_mrm": 5.0,  "pm_cpt": 5.0, "delta": 0.0},
        "À ajouter"   : {"nb": 3, "conf": 2, "pm_mrm": 0.0,  "pm_cpt": 0.0, "delta": 0.0},
        # "À supprimer" NE DOIT PAS entrer dans le total KAS.
        "À supprimer" : {"nb": 9, "conf": 9, "pm_mrm": 99.0, "pm_cpt": 1.0, "delta": 98.0},
    }
    k = kas_totaux({"consignes": cons})
    assert k["nb"] == 6
    assert k["conf"] == 4
    assert k["pm_mrm"] == 15.0
    assert k["delta"] == 6.0
