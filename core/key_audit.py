"""
Audit de solidité des clés de matching — outil de diagnostic READ-ONLY.

À part du pipeline (aucun branchement dans main.py / matching.py, aucune
écriture). Sert à jauger la qualité de `key_strict` avant de s'y fier :

    auditer_cle              — collisions : clés partagées par plusieurs lignes
                               (une collision = faux appariement potentiel).
    cardinalite_cle          — combinatoire : « combien de clés constructibles »
                               (produit des cardinalités des composantes) vs clés
                               réellement observées → pouvoir discriminant.
    tester_substitution_garantie
                             — expérience : pour les lignes compte SANS garantie,
                               forcer GARANTIE = 60 (IT) puis 64 (IP) et compter
                               les matchs MRM. Éclaire le choix d'imputation IP et
                               l'idée « tester des clés en substituant la garantie ».

S'utilise sur les DataFrames nettoyés (cpt_clean / mrm_clean) où les clés
existent (`key_strict`…) et les colonnes sont préfixées CPT_* / MRM_*.
Cf. notebook notebooks/itip_fiab_key_audit.py.
"""

from functools import reduce
from typing import Iterable, List, Tuple

import pandas as pd
from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from config import CODE_GARANTIE_IT, CODE_GARANTIE_IP
from core.transform import normalize_name_full

# Composantes de la clé stricte (suffixes canoniques, à préfixer CPT_ / MRM_).
_STRICT_COMPONENTS: Tuple[str, ...] = (
    "RPP", "D_NAISSANCE", "D_SURVENANCE", "GARANTIE", "NOM_PRENOM",
)


# ============================================================================
# COLLISIONS
# ============================================================================

def auditer_cle(
    df     : DataFrame,
    key_col: str = "key_strict",
    label  : str = "CPT",
    top_n  : int = 10,
) -> pd.DataFrame:
    """
    Mesure la résistance d'une clé aux collisions (clés partagées par >1 ligne).

    Une clé qui colle deux lignes distinctes = appariement potentiellement faux :
    plus le taux de collision est bas, plus la clé est sûre. Imprime un résumé +
    le top des clés les plus collidées, et renvoie le résumé en pandas.

    Returns:
        pandas.DataFrame une ligne : total, nulles, distinctes, clés en collision,
        lignes en collision, multiplicité max, taux de collision (% de lignes
        non nulles partageant leur clé).
    """
    n_total = df.count()
    non_null = df.filter(F.col(key_col).isNotNull())
    n_non_null = non_null.count()
    n_null = n_total - n_non_null

    grp = non_null.groupBy(key_col).count()
    n_distinct = grp.count()
    collisions = grp.filter(F.col("count") > 1)
    n_colliding_keys = collisions.count()
    agg = collisions.agg(
        F.coalesce(F.sum("count"), F.lit(0)).alias("rows"),
        F.coalesce(F.max("count"), F.lit(0)).alias("max_mult"),
    ).first()
    n_colliding_rows = int(agg["rows"] or 0)
    max_mult = int(agg["max_mult"] or 0)
    taux = (n_colliding_rows / n_non_null) if n_non_null else 0.0

    print(f"[audit:{label}] clé={key_col}")
    print(f"  lignes totales        : {n_total:,}")
    print(f"  clés nulles           : {n_null:,}")
    print(f"  clés distinctes       : {n_distinct:,}")
    print(f"  clés en collision     : {n_colliding_keys:,}")
    print(f"  lignes en collision   : {n_colliding_rows:,} ({taux:.2%})")
    print(f"  multiplicité max      : {max_mult}")

    if n_colliding_keys:
        top = (
            collisions.orderBy(F.col("count").desc())
            .limit(top_n).toPandas()
            .rename(columns={key_col: "CLE", "count": "MULTIPLICITE"})
        )
        print(f"  top {min(top_n, n_colliding_keys)} clés collidées :")
        print(top.to_string(index=False))

    return pd.DataFrame([{
        "CLE": key_col, "SOURCE": label,
        "LIGNES_TOTALES": n_total, "CLES_NULLES": n_null,
        "CLES_DISTINCTES": n_distinct, "CLES_EN_COLLISION": n_colliding_keys,
        "LIGNES_EN_COLLISION": n_colliding_rows, "MULTIPLICITE_MAX": max_mult,
        "TAUX_COLLISION": round(taux, 6),
    }])


# ============================================================================
# COMBINATOIRE / POUVOIR DISCRIMINANT
# ============================================================================

def cardinalite_cle(
    df     : DataFrame,
    prefix : str = "CPT_",
    label  : str = "CPT",
    key_col: str = "key_strict",
) -> pd.DataFrame:
    """
    Combinatoire de la clé stricte : « combien de clés je peux construire ».

    Pour chaque composante (rpp, dob, survenance, garantie, nom), compte les
    valeurs distinctes ; le MAX THÉORIQUE de clés = produit de ces cardinalités.
    Comparé aux clés réellement observées, donne le taux d'occupation de l'espace
    (très bas = composantes corrélées / espace creux = clé large mais peu remplie).

    Returns:
        pandas.DataFrame : une ligne par composante (valeurs distinctes) + une
        ligne récap (MAX_THEORIQUE, CLES_OBSERVEES, TAUX_OCCUPATION).
    """
    cols = [c for c in _STRICT_COMPONENTS if f"{prefix}{c}" in df.columns]
    exprs = [F.countDistinct(F.col(f"{prefix}{c}")).alias(c) for c in cols]
    if key_col in df.columns:
        exprs.append(F.countDistinct(F.col(key_col)).alias("_observed"))
    row = df.select(*exprs).first()

    distincts = {c: int(row[c] or 0) for c in cols}
    theorique = reduce(lambda a, b: a * b, distincts.values(), 1)
    observed = int(row["_observed"]) if key_col in df.columns else None
    occupation = (observed / theorique) if (observed is not None and theorique) else None

    print(f"[audit:{label}] combinatoire clé={key_col} (préfixe {prefix})")
    for c, n in distincts.items():
        print(f"  distinct({c:<14}) : {n:,}")
    print(f"  MAX THÉORIQUE         : {theorique:,}")
    if observed is not None:
        print(f"  CLÉS OBSERVÉES        : {observed:,}")
        print(f"  TAUX D'OCCUPATION     : {occupation:.2e}")

    rows = [{"COMPOSANTE": c, "VALEURS_DISTINCTES": n} for c, n in distincts.items()]
    rows.append({"COMPOSANTE": "MAX_THEORIQUE", "VALEURS_DISTINCTES": theorique})
    if observed is not None:
        rows.append({"COMPOSANTE": "CLES_OBSERVEES", "VALEURS_DISTINCTES": observed})
        rows.append({"COMPOSANTE": "TAUX_OCCUPATION", "VALEURS_DISTINCTES": occupation})
    return pd.DataFrame(rows)


# ============================================================================
# EXPÉRIENCE : SUBSTITUTION DE GARANTIE (read-only)
# ============================================================================

def _strict_key(df: DataFrame, prefix: str, garantie_expr: Column) -> Column:
    """Reconstruit la clé stricte (rpp+dob+survenance+garantie+nom) avec une
    expression de garantie au choix — concat_ws ignore les NULL (comme la clé prod)."""
    return F.concat_ws(
        "",
        F.col(f"{prefix}RPP").cast("string"),
        F.date_format(F.col(f"{prefix}D_NAISSANCE"), "yyyyMMdd"),
        F.date_format(F.col(f"{prefix}D_SURVENANCE"), "yyyyMMdd"),
        garantie_expr,
        normalize_name_full(F.col(f"{prefix}NOM_PRENOM")),
    )


def tester_substitution_garantie(
    df_cpt            : DataFrame,
    df_mrm            : DataFrame,
    codes             : Iterable[int] = (CODE_GARANTIE_IT, CODE_GARANTIE_IP),
    prefix_cpt        : str = "CPT_",
    prefix_mrm        : str = "MRM_",
    only_null_garantie: bool = True,
) -> pd.DataFrame:
    """
    Expérience READ-ONLY : pour les lignes compte SANS garantie, forcer la
    garantie à chaque code (60 = IT, 64 = IP) et compter les matchs MRM.

    Le côté MRM garde sa garantie RÉELLE (clé stricte normale, dédoublonnée). On
    substitue uniquement côté CPT → forcer 60 vs 64 donne des résultats DIFFÉRENTS
    et révèle, pour chaque ligne sans garantie, l'hypothèse qui trouve une
    contrepartie : combien de dossiers sont en réalité IT (matchent à 60) vs IP
    (matchent à 64). Éclaire l'imputation IP et l'idée d'étendre au-delà de 60/64.

    Args:
        only_null_garantie : True = ne tester que les lignes compte à garantie
                             nulle/vide (population réellement concernée par
                             l'imputation). False = toutes les lignes compte.

    Returns:
        pandas.DataFrame : une ligne par code (POP_TESTEE, MATCHS, TAUX).
    """
    # MRM : clé stricte réelle, construite une fois et dédoublonnée.
    mrm_key = _strict_key(df_mrm, prefix_mrm, F.col(f"{prefix_mrm}GARANTIE").cast("int"))
    mrm_keys = (
        df_mrm.withColumn("_k", mrm_key)
        .filter(F.col("_k").isNotNull())
        .select("_k").dropDuplicates(["_k"])
    )
    mrm_b = F.broadcast(mrm_keys)

    cpt = df_cpt
    if only_null_garantie:
        g = F.col(f"{prefix_cpt}GARANTIE")
        cpt = cpt.filter(g.isNull() | (F.trim(g.cast("string")) == F.lit("")))
    cpt = cpt.cache()
    n_pop = cpt.count()
    print(f"[audit] substitution garantie : population testée = {n_pop:,} "
          f"({'garantie nulle' if only_null_garantie else 'toutes lignes'})")

    rows: List[dict] = []
    for code in codes:
        cpt_k = cpt.withColumn("_k", _strict_key(cpt, prefix_cpt, F.lit(int(code))))
        n_match = (
            cpt_k.filter(F.col("_k").isNotNull())
            .join(mrm_b, on="_k", how="inner")
            .select("_k").distinct().count()
        )
        taux = (n_match / n_pop) if n_pop else 0.0
        libelle = "IT" if code == CODE_GARANTIE_IT else "IP" if code == CODE_GARANTIE_IP else "?"
        print(f"  garantie={code} (={libelle}) : {n_match:,} matchs ({taux:.2%})")
        rows.append({"GARANTIE_SUBSTITUEE": int(code), "POP_TESTEE": n_pop,
                     "MATCHS": n_match, "TAUX": round(taux, 6)})

    cpt.unpersist()
    return pd.DataFrame(rows)
