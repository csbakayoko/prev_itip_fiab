"""Tests de core.io.sources — résolution des chemins source (sans réseau)."""

import pytest

pytest.importorskip("pyspark")

from core.io.sources import is_sharepoint_path, resolve_source_path, _to_local


def test_is_sharepoint_path():
    assert is_sharepoint_path("sharepoint:/Documents partages/MRM/f.xlsx")
    assert is_sharepoint_path("SharePoint:/x.xlsx")          # insensible à la casse
    assert not is_sharepoint_path("dbfs:/FileStore/x.csv")
    assert not is_sharepoint_path(None)
    assert not is_sharepoint_path("")


def test_resolve_source_path_laisse_passer_les_chemins_non_sharepoint():
    # Un chemin dbfs:/ ressort tel quel, sans toucher à Spark ni au réseau.
    path = "dbfs:/FileStore/MRM_FILES/MRM_Fiab_31_12_23_V3.csv"
    assert resolve_source_path(None, path, {}, "dbfs:/tmp/staging") == path


def test_resolve_source_path_refuse_si_sharepoint_desactive():
    # Voie coupée (mode actuel : dépôt manuel) → refus explicite, pas de réseau.
    with pytest.raises(ValueError, match="désactivée"):
        resolve_source_path(
            None, "sharepoint:/MRM/f.xlsx", {"actif": False}, "dbfs:/tmp/staging",
        )


def test_resolve_source_path_refuse_config_incomplete():
    # Voie active mais app registration absente → erreur explicite, pas de réseau.
    with pytest.raises(ValueError, match="SHAREPOINT incomplète"):
        resolve_source_path(
            None, "sharepoint:/MRM/f.xlsx", {"actif": True, "tenant_id": "t"},
            "dbfs:/tmp/staging",
        )


def test_to_local():
    assert _to_local("dbfs:/FileStore/x.xlsx") == "/dbfs/FileStore/x.xlsx"
    assert _to_local("/tmp/x.xlsx") == "/tmp/x.xlsx"
