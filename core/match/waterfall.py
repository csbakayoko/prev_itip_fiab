"""
Cascade principale de réconciliation CPT/MRM + consignes métier.

Flux (matching_waterfall) :
    CPT + MRM
        ├─ Pre-filter   : MATCH_EXACT / WINDOW / TRONC / TRONC_WINDOW
        ├─ Filtrage MRM : MRM_DELETE → écarté
        ├─ Post-filter  : MATCH_IP / RECHUTE / RECHUTE_TRONC
        ├─ Clé clause   : MATCH_CLAUSE[_WINDOW/_TRONC/_TRONC_WINDOW] (secours RPP nul)
        └─ Orphelins    : CPT_ONLY / MRM_MISSING → union finale

Pour ajouter une étape : copier 3-5 lignes dans la zone correspondante avec
la nouvelle clé (et `extra_cond=...` si une condition supplémentaire est requise).
"""

from functools import reduce
from typing import List, Tuple

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from config import WINDOW_DAYS, IP_GARANTIE_OFFSET, RELAPSE_WINDOW_DAYS, LOG_VOLUMETRIE
from core._timing import timed_fn
from core.match.steps import (
    _materialize, execute_matching_step, _windowed, _ip_cond, _rechute_cond,
)


# ============================================================================
# CATÉGORISATION DES CONSIGNES MRM
# ============================================================================

def categorize_mrm_conclusion(col: Column) -> Column:
    """
    Catégorise la conclusion MRM selon les consignes métier.

    Retient :
        MRM_KEEP             → PM MRM à conserver
        MRM_ADD / MRM_STUDY  → PM à ajouter / à étudier

    Écarte :
        MRM_DELETE           → PM MRM à supprimer
    """
    text = F.lower(F.trim(col))
    return (
        F.when(text.contains("pm mrm à conserver"),                    "MRM_KEEP")
        .when(text.contains("pm à ajouter"),                           "MRM_ADD")
        .when(text.contains("pm dont l'ajout est à étudier"),          "MRM_ADD")
        .when(text.contains("pm mrm à étudier"),                       "MRM_STUDY")
        .when(text.contains("psap à conserver et pm mrm à supprimer"), "MRM_DELETE")
        .when(text.contains("pm mrm à supprimer"),                     "MRM_DELETE")
        .otherwise(None)
    )


def filter_mrm_by_action(
    df_mrm        : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
) -> Tuple[DataFrame, DataFrame]:
    """
    Sépare les MRM selon leur consigne métier.

        MRM_DELETE                       → df_to_remove (TYPE_RECONCILIATION=MRM_DELETE)
        MRM_KEEP / STUDY / ADD / None    → df_to_process
    """
    if conclusion_col not in df_mrm.columns:
        raise ValueError(f"Colonne '{conclusion_col}' absente du DataFrame MRM.")

    df_categorized = df_mrm.withColumn("MRM_ACTION", categorize_mrm_conclusion(F.col(conclusion_col)))
    is_delete = F.col("MRM_ACTION") == "MRM_DELETE"

    df_to_remove = (
        df_categorized.filter(is_delete)
        .withColumn("TYPE_RECONCILIATION", F.lit("MRM_DELETE"))
        .drop("MRM_ACTION")
    )
    # "!= MRM_DELETE" évalue NULL pour les MRM_ACTION nulles → exclus silencieusement.
    df_to_process = (
        df_categorized.filter(~is_delete | F.col("MRM_ACTION").isNull())
        .drop("MRM_ACTION")
    )
    return df_to_remove, df_to_process


# ============================================================================
# ORPHELINS FINAUX
# ============================================================================

def tag_orphans(df_cpt: DataFrame, df_mrm: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Tague CPT_ONLY / MRM_MISSING sur les résiduels post-matching."""
    return (
        df_cpt.withColumn("TYPE_RECONCILIATION", F.lit("CPT_ONLY")),
        df_mrm.withColumn("TYPE_RECONCILIATION", F.lit("MRM_MISSING")),
    )


# ============================================================================
# WATERFALL PRINCIPAL
# ============================================================================

@timed_fn("matching_waterfall")
def matching_waterfall(df_cpt_clean: DataFrame, df_mrm_clean: DataFrame) -> DataFrame:
    """
    Cascade de réconciliation CPT/MRM complète.

    À chaque étape, les lignes matchées sortent et les lignes non matchées
    (remaining) passent à l'étape suivante. Les résiduels finaux deviennent
    des orphelins CPT_ONLY / MRM_MISSING.
    """
    print("[matching] === waterfall démarré ===")

    spark = df_cpt_clean.sparkSession
    # Filet de sécurité : plafonne la taille du plan-string sérialisé par AQE
    # (cause directe de l'OOM driver dans explainStringLocal). Databricks
    # recommande explicitement ce réglage pour les OutOfMemory sur plan.
    try:
        spark.conf.set("spark.sql.maxPlanStringLength", "8k")
    except Exception:
        pass

    # Checkpoint initial : matérialise depuis la source + lignée propre pour la
    # cascade (les étapes suivantes se matérialisent à leur tour).
    df_cpt = _materialize(df_cpt_clean)
    df_mrm = _materialize(df_mrm_clean)
    if LOG_VOLUMETRIE:
        print(f"[matching] entrée : CPT={df_cpt.count():,} | MRM={df_mrm.count():,}")

    results: List[DataFrame] = []
    cpt_rem, mrm_rem = df_cpt, df_mrm

    # === Pre-filter ===
    print("[matching] -- phase pre-filter --")

    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_strict", "MATCH_EXACT",
    )
    results.append(matched)

    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_no_date", "MATCH_WINDOW",
        extra_cond=_windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS),
    )
    results.append(matched)

    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_strict_tronc", "MATCH_TRONC",
    )
    results.append(matched)

    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_no_date_tronc", "MATCH_TRONC_WINDOW",
        extra_cond=_windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS),
    )
    results.append(matched)

    # === Filtrage MRM_DELETE ===
    print("[matching] -- filtrage MRM_DELETE --")
    mrm_removed, mrm_rem = filter_mrm_by_action(mrm_rem)
    results.append(mrm_removed)

    # === Post-filter ===
    print("[matching] -- phase post-filter --")

    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_no_garantie", "MATCH_IP",
        extra_cond=_ip_cond(IP_GARANTIE_OFFSET),
    )
    results.append(matched)

    # MATCH_RECHUTE : même clé que MATCH_WINDOW (rpp+dob+garantie+nom) — la
    # contrainte garantie est désormais portée par la clé. _rechute_cond ne
    # filtre plus que sur la fenêtre de jours (la check garantie reste mais
    # devient redondante, inoffensive).
    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_no_date", "MATCH_RECHUTE",
        extra_cond=_rechute_cond(RELAPSE_WINDOW_DAYS),
    )
    results.append(matched)

    # MATCH_RECHUTE_TRONC : variante de MATCH_RECHUTE sur la clé tronquée
    # (nom CPT coupé à 20 caractères) pour rattraper les rechutes dont le
    # prénom long fait tomber la clé full out.
    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_no_date_tronc", "MATCH_RECHUTE_TRONC",
        extra_cond=_rechute_cond(RELAPSE_WINDOW_DAYS),
    )
    results.append(matched)

    # === Clé de secours « clause » (RPP nul / mal renseigné) ===
    # En dernier : ne rattrape que le résidu non matché par les clés RPP. La
    # clause remplace le RPP ; mêmes variantes que le pré-filtre (strict /
    # fenêtre × nom complet / tronqué). Les clés clause valent NULL côté CPT
    # quand la clause manque → execute_matching_step les ignore (filter isNotNull).
    print("[matching] -- phase clé clause (secours RPP) --")

    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_clause_strict", "MATCH_CLAUSE",
    )
    results.append(matched)

    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_clause_no_date", "MATCH_CLAUSE_WINDOW",
        extra_cond=_windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS),
    )
    results.append(matched)

    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_clause_strict_tronc", "MATCH_CLAUSE_TRONC",
    )
    results.append(matched)

    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_clause_no_date_tronc", "MATCH_CLAUSE_TRONC_WINDOW",
        extra_cond=_windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS),
    )
    results.append(matched)

    # === Orphelins finaux ===
    print("[matching] -- orphelins --")
    cpt_orphans, mrm_critiques = tag_orphans(cpt_rem, mrm_rem)
    results.extend([cpt_orphans, mrm_critiques])

    # === Union finale ===
    print("[matching] -- union finale --")
    df_final = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True), results
    ).cache()
    # Comptage informatif (la cache se matérialise de toute façon en aval) → gaté.
    if LOG_VOLUMETRIE:
        print(f"[matching]   ↳ union : {df_final.count():,} lignes")
    print("[matching] === waterfall terminé ===")
    return df_final
