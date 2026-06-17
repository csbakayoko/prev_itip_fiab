"""Tests de la préparation des données (imputation IP, casts, clés)."""

import datetime as dt

import pytest

pytest.importorskip("pyspark")

from core.transform import impute_garantie_ip, cast_amounts, add_matching_keys
from config import CODE_GARANTIE_IP


def test_impute_garantie_ip(spark):
    df = spark.createDataFrame(
        [
            ("1", None, dt.date(2020, 1, 1)),   # garantie nulle + invalidité → IP
            ("2", "60", dt.date(2020, 1, 1)),   # garantie présente → inchangée
            ("3", None, None),                  # nulle sans invalidité → reste nulle
            ("4", "  ", dt.date(2020, 1, 1)),   # vide/blanche + invalidité → IP
        ],
        ["RPP", "GARANTIE", "D_INVALIDITE"],
    )
    out = {r["RPP"]: r["GARANTIE"] for r in impute_garantie_ip(df).collect()}
    assert out["1"] == str(CODE_GARANTIE_IP)
    assert out["2"] == "60"
    assert out["3"] is None
    assert out["4"] == str(CODE_GARANTIE_IP)


def test_cast_amounts_virgule_europeenne_et_colonne_absente(spark):
    df = spark.createDataFrame([("12,34", "100.5")], ["PM", "PSAP"])
    out = cast_amounts(df, ["PM", "PSAP", "ABSENTE"]).first()   # ABSENTE ignorée (garde)
    assert abs(out["PM"] - 12.34) < 1e-9
    assert abs(out["PSAP"] - 100.5) < 1e-9


def test_add_matching_keys_inclut_la_garantie(spark):
    df = spark.createDataFrame(
        [("R1", "DUPONT JEAN", dt.date(1980, 5, 1), dt.date(2020, 1, 1), "64", "CPB_121981")],
        ["RPP", "NOM_PRENOM", "D_NAISSANCE", "D_SURVENANCE", "GARANTIE", "CLAUSE"],
    )
    row = add_matching_keys(df, rpp_col="RPP").first()
    # key_strict = rpp + dob + survenance + garantie + nom (normalisé, sans espaces)
    assert row["key_strict"] == "R1" + "19800501" + "20200101" + "64" + "DUPONTJEAN"
    # la clé clause remplace le RPP par la clause normalisée (préfixe retiré)
    assert row["key_clause_strict"].startswith("121981")
