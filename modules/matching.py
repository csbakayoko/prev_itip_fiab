"""
Logique métier de réconciliation en cascade (waterfall).

Flux :
    CPT + MRM
        │
        ├─ Pre-filter   : MATCH_EXACT / WINDOW / TRONC / TRONC_WINDOW
        ├─ Filtrage MRM : MRM_DELETE → écarté
        ├─ Post-filter  : MATCH_IP / RECHUTE / DATE_RETARD
        └─ Orphelins    : CPT_ONLY / MRM_MISSING
                │
                └─ Union finale → DataFrame réconcilié

Perf : MRM est broadcasté (taille < 100k). Chaque étape devient un broadcast
hash join sans shuffle. Aucune matérialisation intermédiaire (persist/count) —
seul .cache() lazy sur le DataFrame matché qui est réutilisé 3x (2 anti-joins
+ union finale).

Pour ajouter une étape : copier 3 lignes dans matching_waterfall avec la
nouvelle clé (et extra_cond si besoin).
"""

from functools import reduce
from typing import Callable, List, Optional, Tuple

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from config import WINDOW_DAYS, IP_GARANTIE_OFFSET, RELAPSE_WINDOW_DAYS


# ============================================================================
# CONSTANTES INTERNES
# ============================================================================

_UID_CPT = "_cpt_uid"
_UID_MRM = "_mrm_uid"

_MATCHING_KEYS: Tuple[str, ...] = (
    "key_strict",
    "key_no_date",
    "key_strict_tronc",
    "key_no_date_tronc",
    "key_no_garantie",
    "key_no_date_no_garantie",
)


# ============================================================================
# CONDITIONS RÉUTILISABLES
# ============================================================================

def _windowed(date_cpt: str, date_mrm: str, max_days: int) -> Callable[[], Column]:
    """Fenêtre symétrique : |datediff(CPT, MRM)| <= max_days, dates non nulles."""
    return lambda: (
        (F.abs(F.datediff(F.col(date_cpt), F.col(date_mrm))) <= int(max_days))
        & F.col(date_cpt).isNotNull()
        & F.col(date_mrm).isNotNull()
    )


def _ip_cond(offset: int,
             garantie_cpt: str = "CPT_GARANTIE",
             garantie_mrm: str = "MRM_GARANTIE") -> Callable[[], Column]:
    """Passage IT → IP : |garantie_CPT − garantie_MRM| == offset (cast int)."""
    return lambda: (
        F.abs(F.col(garantie_cpt).cast("int") - F.col(garantie_mrm).cast("int"))
        == F.lit(offset)
    )


def _rechute_cond(relapse_days: int,
                  date_cpt: str = "CPT_D_SURVENANCE",
                  date_mrm: str = "MRM_D_SURVENANCE",
                  garantie_cpt: str = "CPT_GARANTIE",
                  garantie_mrm: str = "MRM_GARANTIE") -> Callable[[], Column]:
    """Rechute IT : même garantie + 0 < |datediff| <= relapse_days."""
    def build() -> Column:
        ecart = F.abs(F.datediff(F.col(date_cpt), F.col(date_mrm)))
        return (
            (F.col(garantie_cpt).cast("int") == F.col(garantie_mrm).cast("int"))
            & (ecart > 0) & (ecart <= F.lit(relapse_days))
            & F.col(date_cpt).isNotNull() & F.col(date_mrm).isNotNull()
            & F.col(garantie_cpt).isNotNull() & F.col(garantie_mrm).isNotNull()
        )
    return build


def _date_retard_cond(min_days: int,
                      date_cpt: str = "CPT_D_SURVENANCE",
                      date_mrm: str = "MRM_D_SURVENANCE") -> Callable[[], Column]:
    """Date MRM en retard : datediff(CPT, MRM) > min_days (CPT strictement plus récent)."""
    return lambda: (
        (F.datediff(F.col(date_cpt), F.col(date_mrm)) > F.lit(min_days))
        & F.col(date_cpt).isNotNull()
        & F.col(date_mrm).isNotNull()
    )


# ============================================================================
# HELPERS SPARK
# ============================================================================

def _alias_mrm_keys(df_mrm: DataFrame) -> DataFrame:
    """Préfixe les clés MRM (_mrm_*) pour éliminer les collisions de colonnes au join."""
    for k in _MATCHING_KEYS:
        if k in df_mrm.columns:
            df_mrm = df_mrm.withColumnRenamed(k, f"_mrm_{k}")
    return df_mrm


def _drop_mrm_keys(df: DataFrame) -> DataFrame:
    """Supprime les clés MRM aliassées une fois le join effectué."""
    return df.drop(*[f"_mrm_{k}" for k in _MATCHING_KEYS if f"_mrm_{k}" in df.columns])


# ============================================================================
# EXÉCUTION D'UNE ÉTAPE DE MATCHING
# ============================================================================

def execute_matching_step(
    df_cpt   : DataFrame,
    df_mrm   : DataFrame,
    key      : str,
    label    : str,
    extra_cond: Optional[Callable[[], Column]] = None,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Une étape : join sur `key` (+ `extra_cond` optionnelle), MRM broadcasté.

    df_mrm doit déjà porter les clés aliasées `_mrm_*` (cf. _alias_mrm_keys).
    Retourne (df_matched, df_cpt_remaining, df_mrm_remaining).
    """
    print(f"[matching] ▶ {label} (clé={key})")

    cond = F.col(key) == F.col(f"_mrm_{key}")
    if extra_cond is not None:
        cond = cond & extra_cond()

    df_matched = (
        df_cpt.join(F.broadcast(df_mrm.filter(F.col(f"_mrm_{key}").isNotNull())),
                    on=cond, how="inner")
              .transform(_drop_mrm_keys)
              .withColumn("TYPE_RECONCILIATION", F.lit(label))
    ).cache()

    # count() force la matérialisation : on voit la progression en temps réel
    # et le .cache() est chaud pour les deux anti-joins + l'union finale.
    n_matched = df_matched.count()
    print(f"[matching]   ↳ {label} : {n_matched:,} matchs")

    df_cpt_rem = df_cpt.join(F.broadcast(df_matched.select(_UID_CPT)), _UID_CPT, "left_anti")
    df_mrm_rem = df_mrm.join(F.broadcast(df_matched.select(_UID_MRM)), _UID_MRM, "left_anti")
    return df_matched, df_cpt_rem, df_mrm_rem


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

        MRM_DELETE                       → df_to_remove (taggué TYPE_RECONCILIATION=MRM_DELETE)
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

def matching_waterfall(df_cpt_clean: DataFrame, df_mrm_clean: DataFrame) -> DataFrame:
    """
    Cascade de réconciliation CPT/MRM complète.

    Pour ajouter une étape : copier 3 lignes dans la zone correspondante avec
    la nouvelle clé (et `extra_cond=...` si une condition supplémentaire est
    requise).
    """
    print("[matching] === waterfall démarré ===")

    # Cache initial CRUCIAL : sans ça, chaque étape re-scanne df_cpt_clean
    # depuis la source (Parquet/Delta) → 8+ scans complets = des dizaines
    # de minutes. Un seul cache au début = un seul scan, tout le reste lit
    # la RAM. monotonically_increasing_id() doit être figé avant cache
    # (non-déterministe sinon → fuites inter-étapes).
    df_cpt = df_cpt_clean.withColumn(_UID_CPT, F.monotonically_increasing_id()).cache()
    df_mrm = _alias_mrm_keys(df_mrm_clean.withColumn(_UID_MRM, F.monotonically_increasing_id())).cache()
    n_cpt, n_mrm = df_cpt.count(), df_mrm.count()
    print(f"[matching] entrée : CPT={n_cpt:,} | MRM={n_mrm:,}")

    results: List[DataFrame] = []
    cpt_rem, mrm_rem = df_cpt, df_mrm

    # === Pre-filter ===
    print("[matching] -- phase pre-filter --")

    # Match exact (date jour + nom complet)
    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_strict", "MATCH_EXACT",
    )
    results.append(matched)

    # Fenêtre ±N jours sur la date (nom complet)
    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_no_date", "MATCH_WINDOW",
        extra_cond=_windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS),
    )
    results.append(matched)

    # Troncature 20 chars du nom (date jour)
    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_strict_tronc", "MATCH_TRONC",
    )
    results.append(matched)

    # Troncature 20 chars + fenêtre ±N jours
    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_no_date_tronc", "MATCH_TRONC_WINDOW",
        extra_cond=_windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS),
    )
    results.append(matched)

    # === Zone d'ajout d'étapes pre-filter ===
    # matched, cpt_rem, mrm_rem = execute_matching_step(
    #     cpt_rem, mrm_rem, "ma_cle", "MON_LABEL",
    # )
    # results.append(matched)

    # === Filtrage des consignes MRM (MRM_DELETE écartés) ===
    print("[matching] -- filtrage MRM_DELETE --")
    mrm_removed, mrm_rem = filter_mrm_by_action(mrm_rem)
    results.append(mrm_removed)

    # === Post-filter ===
    print("[matching] -- phase post-filter --")

    # Passage IT → IP : même nom/date, garantie décalée de IP_GARANTIE_OFFSET
    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_no_garantie", "MATCH_IP",
        extra_cond=_ip_cond(IP_GARANTIE_OFFSET),
    )
    results.append(matched)

    # Rechute : même garantie, écart de date ≤ RELAPSE_WINDOW_DAYS (et > 0)
    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_no_date_no_garantie", "MATCH_RECHUTE",
        extra_cond=_rechute_cond(RELAPSE_WINDOW_DAYS),
    )
    results.append(matched)

    # Date MRM en retard : CPT strictement plus récente de > WINDOW_DAYS
    matched, cpt_rem, mrm_rem = execute_matching_step(
        cpt_rem, mrm_rem, "key_no_date", "MATCH_DATE_RETARD",
        extra_cond=_date_retard_cond(WINDOW_DAYS),
    )
    results.append(matched)

    # === Zone d'ajout d'étapes post-filter ===
    # matched, cpt_rem, mrm_rem = execute_matching_step(
    #     cpt_rem, mrm_rem, "ma_cle", "MON_LABEL", extra_cond=...,
    # )
    # results.append(matched)

    # === Orphelins finaux ===
    print("[matching] -- orphelins --")
    cpt_orphans, mrm_critiques = tag_orphans(cpt_rem, mrm_rem)
    results.extend([cpt_orphans, mrm_critiques])

    # === Union finale ===
    # Matérialiser ici (cache + count) attribue le temps des orphelins et de
    # l'union au waterfall plutôt qu'à la première action du caller (write…).
    print("[matching] -- union finale (orphelins + matched) --")
    df_final = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), results).cache()
    n_final = df_final.count()
    print(f"[matching]   ↳ union : {n_final:,} lignes")
    print("[matching] === waterfall terminé ===")
    return df_final.drop(_UID_CPT, _UID_MRM)


# ============================================================================
# DÉCLARATIONS TARDIVES (MRM N+1, N+2, …)
# ============================================================================

def recover_late_declarations(
    df_result  : DataFrame,
    inventories: List[Tuple[str, DataFrame]],
    key        : str = "key_no_date",
) -> DataFrame:
    """
    Donne une seconde chance aux CPT_ONLY sur des inventaires MRM ultérieurs.

    Cascade : chaque CPT_ONLY est testé contre les inventaires dans l'ordre.
    Le premier inventaire qui contient la clé récupère le dossier :
        - TYPE_RECONCILIATION → "CPT_LATE"
        - LATE_SOURCE         → tag de l'inventaire (ex: "MRM_N1")
        - colonnes MRM_*      → enrichies depuis l'inventaire
    Les non retrouvés restent CPT_ONLY.

    Chaque inventaire est dédoublonné sur la clé et broadcasté.
    """
    print("[late] === recovery démarré ===")
    is_cpt_only = F.col("TYPE_RECONCILIATION") == "CPT_ONLY"
    rest = df_result.filter(~is_cpt_only)

    # Cache initial : remaining_cpt est lu 2× par inventaire (join + anti-join).
    remaining_cpt = (
        df_result.filter(is_cpt_only)
                 .select(*[c for c in df_result.columns if not c.startswith("MRM_")])
    ).cache()
    n_init = remaining_cpt.count()
    print(f"[late] CPT_ONLY initiaux : {n_init:,}")

    recovered: List[DataFrame] = []
    for tag, df_mrm in inventories:
        print(f"[late] ▶ {tag}")
        mrm_enrich = (
            df_mrm.filter(F.col(key).isNotNull())
                  .select(key, *[c for c in df_mrm.columns if c.startswith("MRM_")])
                  .dropDuplicates([key])
        )
        hit = (
            remaining_cpt.join(F.broadcast(mrm_enrich), on=key, how="inner")
                         .withColumn("TYPE_RECONCILIATION", F.lit("CPT_LATE"))
                         .withColumn("LATE_SOURCE", F.lit(tag))
        ).cache()
        # count() force la matérialisation : warm cache + visibilité.
        n_hit = hit.count()
        print(f"[late]   ↳ {tag} : {n_hit:,} retrouvés")

        # Anti-join broadcast — hit.select(key) reste tout petit même si hit est gros.
        remaining_cpt = remaining_cpt.join(
            F.broadcast(hit.select(key).distinct()), on=key, how="left_anti"
        )
        recovered.append(hit)

    # Matérialiser le résultat ici évite de différer le travail au caller (display/write).
    df_final = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [rest, remaining_cpt, *recovered],
    ).cache()
    n_final = df_final.count()
    print(f"[late]   ↳ union : {n_final:,} lignes")
    print("[late] === recovery terminé ===")
    return df_final
