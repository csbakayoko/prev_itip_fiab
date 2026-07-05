"""Tests des dimensions d'export (CLAUSE / TYPE_COMPTE / REMONTE_DF) — Spark local."""

import pytest

pytest.importorskip("pyspark")

from core.match.recovery import derive_clause_column, enrich_result_tags


def test_derive_clause_et_type_compte(spark):
    df = spark.createDataFrame(
        [
            ("CPB_121981", None,  None),    # PB mappé via le préfixe CPT
            ("CXY_777",    None,  None),    # préfixe NON mappé (futur HPB, …)
            ("CPB_111",    "222", "HPB"),   # MRM prioritaire (clause + type)
            (None,         None,  None),    # aucune info clause
        ],
        "CPT_CLAUSE string, MRM_CLAUSE string, MRM_TYPE_CLAUSE string",
    )
    rows = {r["CPT_CLAUSE"]: r for r in derive_clause_column(df).collect()}

    assert rows["CPB_121981"]["CLAUSE"] == "121981"
    assert rows["CPB_121981"]["TYPE_COMPTE"] == "PB"
    # Type non mappé → PRÉFIXE BRUT visible dans les exports, jamais null muet.
    assert rows["CXY_777"]["TYPE_COMPTE"] == "CXY"
    # La revue MRM est prioritaire quand elle porte la clause et le type.
    assert rows["CPB_111"]["CLAUSE"] == "222"
    assert rows["CPB_111"]["TYPE_COMPTE"] == "HPB"
    # Pas de clause = donnée légitime (nullable), pas une erreur.
    assert rows[None]["CLAUSE"] is None
    assert rows[None]["TYPE_COMPTE"] is None


def test_derive_clause_idempotente(spark):
    df = spark.createDataFrame([("CPB_1",)], "CPT_CLAUSE string")
    once  = derive_clause_column(df)
    twice = derive_clause_column(once)
    assert once.columns == twice.columns          # passthrough, pas de doublon


def test_enrich_result_tags_porte_les_dimensions(spark):
    df = spark.createDataFrame(
        [
            ("MATCH_EXACT", "CPB_1", "1", "NON", "pm mrm à conserver", 100.0, None),
            ("MATCH_EXACT", "CPB_1", "1", "OUI", "pm mrm à conserver", 100.0, None),
            ("CPT_ONLY",    "CPB_2", None, None, None,                 50.0,  None),
        ],
        "TYPE_RECONCILIATION string, CPT_CLAUSE string, MRM_CLAUSE string, "
        "MRM_STATUT_INV string, MRM_CONCLUSION string, CPT_PM double, "
        "CPT_D_SURVENANCE date",
    )
    out = enrich_result_tags(df).collect()
    by_statut = {r["MRM_STATUT_INV"]: r for r in out}

    for col in ("CLAUSE", "TYPE_COMPTE", "REMONTE_DF", "MRM_ACTION", "TAG_CPT_ONLY"):
        assert col in out[0].asDict()
    assert by_statut["NON"]["REMONTE_DF"] == "Non"
    assert by_statut["OUI"]["REMONTE_DF"] == "Oui"
    assert by_statut[None]["REMONTE_DF"] is None      # pas d'info MRM
    assert by_statut[None]["TYPE_COMPTE"] == "PB"     # dérivé du préfixe CPT
