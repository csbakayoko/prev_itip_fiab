"""
Croisement orphelins ↔ entrepôt : traçage d'historique et statistiques.

Pour chaque orphelin (CPT_ONLY / MRM_MISSING), on cherche dans l'entrepôt de SA
base toutes ses apparitions sur les différentes dates d'inventaire :

    trace_history  : 1 ligne par (orphelin × apparition entrepôt). Left join →
                     un orphelin absent de l'entrepôt ressort avec _WH_ROW null.
    history_stats  : 1 ligne par orphelin → retrouvé ?, nb d'apparitions,
                     dates min/max, dérive PM/PSAP entre 1re et dernière apparition.
    print_summary  : agrégat console (retrouvés / non retrouvés, distribution).

Clé de traçage par défaut : key_no_date (rpp + naissance + garantie + nom),
stable d'un inventaire à l'autre. Passer key="key_no_garantie" pour suivre un
dossier au travers d'un passage IT→IP (changement de code garantie).
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Colonne marqueur ajoutée à l'entrepôt avant le join : présente (1) côté entrepôt,
# null pour un orphelin sans aucune apparition (left join non résolu).
_WH_ROW = "_WH_ROW"


def trace_history(orphans: DataFrame, warehouse: DataFrame, key: str = "key_no_date") -> DataFrame:
    """
    Associe chaque orphelin à toutes ses apparitions dans l'entrepôt.

    Args:
        orphans   : sortie d'extract_orphans (mêmes clés que l'entrepôt).
        warehouse : sortie de prepare_warehouse (même base).
        key       : clé de jointure (défaut key_no_date).

    Returns:
        DataFrame : clé + colonnes entrepôt + _WH_ROW. Left join sur les orphelins
        distincts → les non-retrouvés sont conservés (colonnes entrepôt nulles).
    """
    seeds = orphans.select(key).filter(F.col(key).isNotNull()).distinct()
    wh    = warehouse.withColumn(_WH_ROW, F.lit(1))
    return seeds.join(wh, on=key, how="left")


def history_stats(traced: DataFrame, base: str, key: str = "key_no_date") -> DataFrame:
    """
    Résume l'historique entrepôt de chaque orphelin (1 ligne par clé).

    Colonnes produites :
        retrouve        : booléen — au moins une apparition entrepôt
        n_apparitions   : nb de dates d'inventaire où le dossier apparaît
        inv_min/inv_max : 1re / dernière date d'inventaire (si dispo)
        pm_first/last   : PM à la 1re / dernière apparition, et pm_drift (last-first)
        psap_first/last : idem PSAP, et psap_drift

    Les colonnes temporelles/montants sont omises si absentes de l'entrepôt
    (ex: l'entrepôt CPT peut ne pas porter de date d'inventaire).
    """
    P    = f"{base.upper()}_"
    inv  = f"{P}D_INVENTAIRE"
    pm   = f"{P}PM"
    psap = f"{P}PSAP"
    has_inv = inv in traced.columns

    aggs = [F.coalesce(F.sum(_WH_ROW), F.lit(0)).alias("n_apparitions")]
    if has_inv:
        aggs += [F.min(inv).alias("inv_min"), F.max(inv).alias("inv_max")]
        # struct(inv, pm, psap) : min/max ordonnent par inv → valeurs à la 1re/dernière
        # apparition récupérées en une seule passe.
        s = F.struct(
            F.col(inv).alias("inv"),
            (F.col(pm)   if pm   in traced.columns else F.lit(None)).alias("pm"),
            (F.col(psap) if psap in traced.columns else F.lit(None)).alias("psap"),
        )
        aggs += [F.min(s).alias("_first"), F.max(s).alias("_last")]

    g = traced.groupBy(key).agg(*aggs)

    out = [
        F.col(key),
        (F.col("n_apparitions") > 0).alias("retrouve"),
        F.col("n_apparitions"),
    ]
    if has_inv:
        out += [F.col("inv_min"), F.col("inv_max")]
        out += [
            F.col("_first.pm").alias("pm_first"),
            F.col("_last.pm").alias("pm_last"),
            (F.col("_last.pm") - F.col("_first.pm")).alias("pm_drift"),
            F.col("_first.psap").alias("psap_first"),
            F.col("_last.psap").alias("psap_last"),
            (F.col("_last.psap") - F.col("_first.psap")).alias("psap_drift"),
        ]
    return g.select(*out)


def print_summary(stats: DataFrame, base: str) -> dict:
    """Agrège et imprime un récap console des orphelins tracés. Retourne les scalaires."""
    row = stats.agg(
        F.count("*").alias("total"),
        F.sum(F.col("retrouve").cast("int")).alias("retrouves"),
        F.avg(F.when(F.col("retrouve"), F.col("n_apparitions"))).alias("moy_apparitions"),
    ).first()

    total     = row["total"] or 0
    retrouves = row["retrouves"] or 0
    pct       = (100.0 * retrouves / total) if total else 0.0

    print(f"\n[investig:{base}] orphelins tracés : {total:,}")
    print(f"[investig:{base}]   ↳ retrouvés dans l'entrepôt : {retrouves:,} ({pct:.1f}%)")
    print(f"[investig:{base}]   ↳ jamais retrouvés          : {total - retrouves:,}")
    if row["moy_apparitions"]:
        print(f"[investig:{base}]   ↳ apparitions moy. (retrouvés) : {row['moy_apparitions']:.1f}")

    return {
        "base": base,
        "total": total,
        "retrouves": retrouves,
        "non_retrouves": total - retrouves,
        "pct_retrouves": pct,
    }
