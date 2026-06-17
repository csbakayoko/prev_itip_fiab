"""
Fixtures partagées des tests.

Le pipeline tourne sur Databricks (PySpark). Les tests qui ont besoin de Spark
utilisent la fixture `spark` (SparkSession locale, 1 cœur). Quand pyspark n'est
pas installé (machine sans le runtime), `importorskip` SKIP proprement ces tests
au lieu de les faire échouer — la CI / Databricks (où pyspark existe) les exécute.
Les tests de logique pure (pandas / Python) tournent partout.
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    """SparkSession locale minimale, réutilisée par toute la session de test."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("itip-fiab-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
