"""
Extraction des orphelins depuis le résultat de réconciliation.

Orphelins = lignes que le waterfall n'a pas réussi à apparier :
    CPT_ONLY     → présent côté Compte, aucun MRM trouvé
    MRM_MISSING  → présent côté MRM, aucun Compte trouvé

Chaque orphelin conserve ses clés de matching (key_*) — c'est par elles qu'on
le retrouvera dans l'entrepôt (cf. investigations/analyze.py).
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


# Tag TYPE_RECONCILIATION posé par tag_orphans() dans le waterfall.
ORPHAN_TAG = {"cpt": "CPT_ONLY", "mrm": "MRM_MISSING"}


def extract_orphans(df_result: DataFrame, base: str) -> DataFrame:
    """
    Isole les orphelins d'une base et ne garde que leurs colonnes utiles.

    Args:
        df_result : sortie de matching_waterfall (+ recover_late_declarations).
        base      : "cpt" ou "mrm".

    Returns:
        DataFrame des orphelins : clés de matching (key_*) + colonnes de la base
        (CPT_* ou MRM_*). Les colonnes de l'autre base, nulles ici, sont retirées.
    """
    if base not in ORPHAN_TAG:
        raise ValueError(f"base inconnue : '{base}'. Attendu : 'cpt' ou 'mrm'.")

    prefix = f"{base.upper()}_"
    keys   = [c for c in df_result.columns if c.startswith("key_")]
    cols   = keys + [c for c in df_result.columns if c.startswith(prefix)]

    return (
        df_result
        .filter(F.col("TYPE_RECONCILIATION") == ORPHAN_TAG[base])
        .select(*cols)
    )
