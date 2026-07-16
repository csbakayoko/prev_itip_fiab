"""Tests de core.io.save_result — validations d'entrée (aucune écriture Delta)."""

import pytest

pytest.importorskip("pyspark")

from core.io.save_result import save_result_delta


def test_refuse_schema_vide():
    with pytest.raises(ValueError, match="delta_schema vide"):
        save_result_delta(None, "", "31/12/2023")


def test_refuse_date_invalide():
    # "n/d" (date non résolue) → on refuse d'historiser à l'aveugle.
    with pytest.raises(ValueError, match="date_inventaire invalide"):
        save_result_delta(None, "hive_metastore.itip_backtest", "n/d")
