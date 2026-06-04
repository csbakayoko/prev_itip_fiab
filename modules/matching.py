"""
Logique métier de réconciliation en cascade (waterfall).

Flux :
    CPT + MRM
        │
        ├─ Pre-filter   : MATCH_EXACT / WINDOW / TRONC / TRONC_WINDOW
        ├─ Filtrage MRM : MRM_DELETE → écarté
        ├─ Post-filter  : MATCH_IP / RECHUTE / RECHUTE_TRONC
        └─ Orphelins    : CPT_ONLY / MRM_MISSING
                │
                └─ Union finale → DataFrame réconcilié

Architecture des joins :
- Join classique sur la clé courante (df_cpt.join(df_mrm, on=key, how="inner")).
- MRM est dédoublonné sur la clé avant chaque join (dropDuplicates([key])) →
  un CPT donné matche au plus 1 MRM (pas de fan-out côté MRM).
- Anti-join sur la clé : les lignes dont la clé apparaît dans matched sont
  retirées de remaining et passent à l'étape suivante.
- MRM broadcasté (taille modérée) → joins sans shuffle.
- Cache + count sur df_cpt/df_mrm en entrée, sur chaque matched, sur df_final
  → matérialisation contrôlée, visibilité dans les logs.

Pour ajouter une étape : copier 3-5 lignes dans la zone correspondante avec
la nouvelle clé (et `extra_cond=...` si une condition supplémentaire est requise).
"""

from functools import reduce
from typing import Callable, List, Optional, Tuple

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from config import WINDOW_DAYS, IP_GARANTIE_OFFSET, RELAPSE_WINDOW_DAYS, LATE_IT_GARANTIE, ORPHAN_FIN_ANNEE_MOIS
from modules._timing import timed_fn


# ============================================================================
# CONSTANTES INTERNES
# ============================================================================

# Toutes les clés de matching présentes des deux côtés (CPT et MRM). Sert à
# nettoyer côté MRM les clés non utilisées par le join courant (sinon collision
# de colonnes dupliquées dans le résultat).
_MATCHING_KEYS: Tuple[str, ...] = (
    "key_strict",
    "key_no_date",
    "key_strict_tronc",
    "key_no_date_tronc",
    "key_no_garantie",
)


# ============================================================================
# CONDITIONS RÉUTILISABLES (appliquées en .filter() après le join)
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


# ============================================================================
# EXÉCUTION D'UNE ÉTAPE DE MATCHING
# ============================================================================

def execute_matching_step(
    df_cpt    : DataFrame,
    df_mrm    : DataFrame,
    key       : str,
    label     : str,
    extra_cond: Optional[Callable[[], Column]] = None,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Join classique sur `key` + filtre `extra_cond` optionnel.

    - MRM dédoublonné sur `key` avant le join → un CPT matche au plus 1 MRM.
    - Les autres clés de matching côté MRM sont retirées avant le join (sinon
      collision de colonnes dupliquées sur les clés non utilisées ici).
    - Anti-join sur `key` : toute ligne dont la clé apparaît dans matched
      sort du remaining.

    Les remaining sont localCheckpoint(eager=True) pour TRONQUER LA LIGNÉE.
    Attention : cache()/persist() ne suffit PAS — un InMemoryRelation expose son
    plan caché comme "inner child", donc la sérialisation du plan
    (AdaptiveSparkPlanExec.onUpdatePlan → explainStringLocal) ré-expose toute la
    chaîne précédente et le plan-string explose quand même (OutOfMemoryError
    driver). localCheckpoint matérialise puis remplace la lignée par une feuille
    RDD opaque, sans plan interne → le plan reste plat à chaque étape.
    """
    print(f"[matching] ▶ {label} (clé={key})")

    # Retirer les autres clés côté MRM pour éviter les ambiguïtés de colonnes.
    other_keys = [k for k in _MATCHING_KEYS if k != key and k in df_mrm.columns]
    df_mrm_join = (
        df_mrm.filter(F.col(key).isNotNull())
              .dropDuplicates([key])
    )
    if other_keys:
        df_mrm_join = df_mrm_join.drop(*other_keys)

    df_matched = df_cpt.join(F.broadcast(df_mrm_join), on=key, how="inner")
    if extra_cond is not None:
        df_matched = df_matched.filter(extra_cond())
    # Checkpoint du matched : matérialise + coupe la lignée (réutilisé pour
    # l'anti-join et l'union finale, sans ré-exposer le plan amont).
    df_matched = (
        df_matched.withColumn("TYPE_RECONCILIATION", F.lit(label))
                  .localCheckpoint(eager=True)
    )
    n_matched = df_matched.count()
    print(f"[matching]   ↳ {label} : {n_matched:,} matchs")

    # Anti-join sur la clé : on retire des remaining les lignes dont la clé est
    # dans matched. localCheckpoint coupe la lignée (cf. docstring).
    matched_keys = F.broadcast(df_matched.select(key).distinct())
    df_cpt_rem = df_cpt.join(matched_keys, on=key, how="left_anti").localCheckpoint(eager=True)
    df_mrm_rem = df_mrm.join(matched_keys, on=key, how="left_anti").localCheckpoint(eager=True)

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
    # cascade (les étapes suivantes localCheckpoint à leur tour).
    df_cpt = df_cpt_clean.localCheckpoint(eager=True)
    df_mrm = df_mrm_clean.localCheckpoint(eager=True)
    n_cpt, n_mrm = df_cpt.count(), df_mrm.count()
    print(f"[matching] entrée : CPT={n_cpt:,} | MRM={n_mrm:,}")

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

    # === Orphelins finaux ===
    print("[matching] -- orphelins --")
    cpt_orphans, mrm_critiques = tag_orphans(cpt_rem, mrm_rem)
    results.extend([cpt_orphans, mrm_critiques])

    # === Union finale ===
    print("[matching] -- union finale --")
    df_final = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True), results
    ).cache()
    n_final = df_final.count()
    print(f"[matching]   ↳ union : {n_final:,} lignes")
    print("[matching] === waterfall terminé ===")
    return df_final


# ============================================================================
# DÉCLARATIONS TARDIVES (MRM N+1, N+2, …)
# ============================================================================

@timed_fn("recover_late_declarations")
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

    Chaque inventaire est dédoublonné sur la clé et broadcasté.
    """
    print("[late] === recovery démarré ===")
    is_cpt_only = F.col("TYPE_RECONCILIATION") == "CPT_ONLY"
    rest = df_result.filter(~is_cpt_only)

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
        n_hit = hit.count()
        print(f"[late]   ↳ {tag} : {n_hit:,} retrouvés")

        remaining_cpt = remaining_cpt.join(
            F.broadcast(hit.select(key).distinct()), on=key, how="left_anti"
        )
        recovered.append(hit)

    df_final = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [rest, remaining_cpt, *recovered],
    ).cache()
    n_final = df_final.count()
    print(f"[late]   ↳ union : {n_final:,} lignes")
    print("[late] === recovery terminé ===")
    return df_final
