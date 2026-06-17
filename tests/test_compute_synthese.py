"""Test de bout en bout du moteur de synthèse sur un df_result synthétique.

Vérifie les invariants critiques (cohérence des classements, volumétrie par
catégorie, taux de chute) sur 4 lignes représentant chaque grand cas.
"""

import pytest

pytest.importorskip("pyspark")

from core.kpi_export import compute_synthese


def _df_result(spark):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    schema = StructType([
        StructField("TYPE_RECONCILIATION", StringType()),
        StructField("MRM_PM",              DoubleType()),
        StructField("CPT_PM",              DoubleType()),
        StructField("MRM_CONCLUSION",      StringType()),
    ])
    data = [
        ("MATCH_EXACT", 100.0, 80.0, "PM MRM à conserver"),   # matché, base de chute
        ("MRM_MISSING",  50.0, None, "PM à ajouter"),         # revue non retrouvée
        ("CPT_ONLY",     None, 30.0, None),                   # anomalie compte
        ("MRM_DELETE",   10.0, None, "PM MRM à supprimer"),   # à supprimer
    ]
    return spark.createDataFrame(data, schema)


def test_invariants_et_taux(spark):
    d = compute_synthese(_df_result(spark))

    # 1. Toutes les lignes tombent dans une catégorie connue.
    assert d["coherent"] is True
    assert d["labels_inconnus"] == []

    # 2. Volumétrie par catégorie.
    assert d["match_nb"] == 1
    assert d["non_mappes_nb"] == 1
    assert d["a_supprimer_nb"] == 1
    assert d["def_nb"] == 1

    # 3. Taux de chute = (PM MRM − PM CPT) / PM MRM sur la base (le seul matché,
    #    hors « à supprimer ») : (100 − 80) / 100 = 20 %.
    assert d["taux_chute_inventaire"] == 20.0
    assert d["metrics_pm_mrm"] == 100.0
    assert d["metrics_pm_cpt"] == 80.0

    # 4. Auto-contrôle interne : chute == Σ consignes + hors consigne.
    assert d["chute_coherente"] is True
