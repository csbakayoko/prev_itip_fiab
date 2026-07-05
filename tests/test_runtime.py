"""Tests de core.runtime.configurer_run — propagation des globals (sans Spark)."""

import pytest

pytest.importorskip("pyspark")          # les modules consommateurs importent pyspark

from core.runtime import configurer_run
from config import RUN_PARAMS
import core.io.load_data as load_data
import core.match.recovery as recovery
import core.synthese.kpi_export as kpi_export


def test_configurer_run_propage_les_globals():
    info = configurer_run(
        date_inventaire="31/12/2024", cpt_vision="CC2024",
        fichier_mrm="dbfs:/x/mrm_2024.csv", fichier_mrm_n1="dbfs:/x/mrm_2024_n1.csv",
    )
    # date lue par recovery (tag obs tardives) ET kpi_export (date synthèse)
    assert recovery.DATE_INVENTAIRE == "31/12/2024"
    assert kpi_export.DATE_INVENTAIRE == "31/12/2024"
    # vision lue par load_cpt_raw
    assert load_data.CLIENT_CPT_VISION == "CC2024"
    # fichiers dans le dict partagé RUN_PARAMS
    assert RUN_PARAMS["fichier_mrm"] == "dbfs:/x/mrm_2024.csv"
    assert RUN_PARAMS["fichier_mrm_n1"] == "dbfs:/x/mrm_2024_n1.csv"
    assert info["date_inventaire"] == "31/12/2024"


def test_configurer_run_sans_n1_retire_la_cle():
    RUN_PARAMS["fichier_mrm_n1"] = "dbfs:/x/ancien_n1.csv"
    configurer_run(
        date_inventaire="31/12/2023", cpt_vision="CC2023",
        fichier_mrm="dbfs:/x/mrm_2023.csv",
    )
    # pas de N+1 → la clé est retirée (sinon récupération tardive parasite)
    assert "fichier_mrm_n1" not in RUN_PARAMS
    assert load_data.CLIENT_CPT_VISION == "CC2023"
