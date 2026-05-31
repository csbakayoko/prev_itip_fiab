"""
Logique métier de réconciliation en cascade (waterfall).

Flux :
    CPT + MRM
        │
        ├─ STEPS_PRE_FILTER   : MATCH_EXACT / WINDOW / TRONC / TRONC_WINDOW
        ├─ Filtrage MRM       : MRM_DELETE → écarté
        ├─ STEPS_POST_FILTER  : MATCH_IP / RECHUTE / DATE_RETARD
        └─ Orphelins          : CPT_ONLY / MRM_MISSING
                │
                └─ Union finale → DataFrame réconcilié

Fonctions publiques :
    matching_waterfall()        — Cascade complète → df_result avec TYPE_RECONCILIATION
    recover_late_declarations() — Seconde chance des CPT_ONLY sur les MRM N+1, N+2…
    categorize_mrm_conclusion() — Catégorise la conclusion MRM brute

Note : derive_clause_column / derive_invalidite_column sont dans core/enrich.py.
"""

from dataclasses import dataclass, field
from functools import reduce
from typing import Callable, List, Optional, Tuple

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from config import WINDOW_DAYS, IP_GARANTIE_OFFSET, RELAPSE_WINDOW_DAYS


# ============================================================================
# CONSTANTES INTERNES
# ============================================================================

# Identifiants de ligne pour anti-join inter-étapes — nécessaires car certaines
# clés de matching ne sont pas uniques (windowed / tronc partagent une clé).
_UID_CPT = "_cpt_uid"
_UID_MRM = "_mrm_uid"

# Toutes les clés de matching présentes des deux côtés (CPT et MRM). Elles sont
# renommées en _mrm_{key} côté MRM avant chaque join pour éviter les colonnes
# dupliquées (sans avoir à passer derrière avec un drop_duplicate_columns).
_MATCHING_KEYS: Tuple[str, ...] = (
    "key_strict",
    "key_no_date",
    "key_strict_tronc",
    "key_no_date_tronc",
    "key_no_garantie",
    "key_no_date_no_garantie",
)


# ============================================================================
# MODÈLE D'ÉTAPE DE MATCHING
# ============================================================================

@dataclass(frozen=True)
class WaterfallStep:
    """
    Étape de matching : égalité sur `join_key` + condition optionnelle `extra_cond`.

    Attributs :
        label      : valeur posée dans TYPE_RECONCILIATION (ex: "MATCH_EXACT")
        join_key   : clé d'égalité (présente sur CPT et MRM)
        extra_cond : fonction renvoyant une Column Spark, évaluée à l'exécution.
                     Doit référencer les colonnes CPT/MRM telles quelles ; la
                     comparaison clé MRM est déjà gérée par _execute_step.
    """
    label    : str
    join_key : str
    extra_cond: Optional[Callable[[], Column]] = field(default=None, compare=False)


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
# ÉTAPES DÉCLARATIVES
# ============================================================================
#
# Pre-filter : matchs stricts/élargis, lancés avant le filtrage des MRM_DELETE.
# Post-filter : récupération métier (passage IT→IP, rechute, date en retard).
# Pour ajouter une étape : ajouter un WaterfallStep dans la bonne liste.
# ============================================================================

STEPS_PRE_FILTER: List[WaterfallStep] = [
    # Égalité stricte (date au jour, NOM_PRENOM complet).
    WaterfallStep("MATCH_EXACT", "key_strict"),

    # Fenêtre ±N jours sur la date de survenance (nom complet).
    WaterfallStep(
        "MATCH_WINDOW", "key_no_date",
        extra_cond=_windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS),
    ),

    # Troncature CPT 20 chars (date jour). CPT limite NOM_PRENOM à 20 caractères,
    # la coupure tombe parfois dans le dernier prénom — les deux côtés appliquent
    # LEFT(20) uppercase + strip espaces.
    WaterfallStep("MATCH_TRONC", "key_strict_tronc"),

    # Troncature CPT 20 chars + fenêtre ±N jours (cumul des deux anomalies).
    WaterfallStep(
        "MATCH_TRONC_WINDOW", "key_no_date_tronc",
        extra_cond=_windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS),
    ),
]

# IP / rechute / date_retard tournent sur les orphelins, après le filtrage MRM.
# La liste effective est construite dans matching_waterfall (skip MATCH_IP si
# ip_offset=None).
STEPS_POST_FILTER: List[WaterfallStep] = [
    WaterfallStep("MATCH_IP", "key_no_garantie",
                  extra_cond=_ip_cond(IP_GARANTIE_OFFSET)),
    WaterfallStep("MATCH_RECHUTE", "key_no_date_no_garantie",
                  extra_cond=_rechute_cond(RELAPSE_WINDOW_DAYS)),
    WaterfallStep("MATCH_DATE_RETARD", "key_no_date",
                  extra_cond=_date_retard_cond(WINDOW_DAYS)),
]


# ============================================================================
# HELPERS SPARK
# ============================================================================

def _persist_count(df: DataFrame) -> DataFrame:
    df.persist()
    df.count()
    return df


def _swap_persist(old: DataFrame, new: DataFrame) -> DataFrame:
    _persist_count(new)
    old.unpersist()
    return new


def _alias_mrm_keys(df_mrm: DataFrame) -> DataFrame:
    """Préfixe toutes les clés de matching côté MRM (_mrm_*) pour éliminer les collisions de colonnes au join."""
    for k in _MATCHING_KEYS:
        if k in df_mrm.columns:
            df_mrm = df_mrm.withColumnRenamed(k, f"_mrm_{k}")
    return df_mrm


def _drop_mrm_keys(df: DataFrame) -> DataFrame:
    """Supprime les clés MRM aliassées une fois le join effectué."""
    return df.drop(*[f"_mrm_{k}" for k in _MATCHING_KEYS if f"_mrm_{k}" in df.columns])


def _exclude_matched(df_cpt: DataFrame, df_mrm: DataFrame, df_matched: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Retire de df_cpt / df_mrm les lignes présentes dans df_matched (via uids)."""
    return (
        df_cpt.join(F.broadcast(df_matched.select(_UID_CPT).distinct()), _UID_CPT, "left_anti"),
        df_mrm.join(F.broadcast(df_matched.select(_UID_MRM).distinct()), _UID_MRM, "left_anti"),
    )


# ============================================================================
# EXÉCUTION D'UNE ÉTAPE
# ============================================================================

def _execute_step(
    df_cpt: DataFrame,
    df_mrm: DataFrame,
    step  : WaterfallStep,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Exécute une étape : égalité sur step.join_key + step.extra_cond optionnelle.
    Les df_cpt / df_mrm en entrée doivent porter les uids posés par matching_waterfall().
    """
    for side, df in (("CPT", df_cpt), ("MRM", df_mrm)):
        if step.join_key not in df.columns:
            raise ValueError(f"Colonne '{step.join_key}' absente du DataFrame {side}.")

    df_cpt_valid = df_cpt.filter(F.col(step.join_key).isNotNull())
    df_mrm_valid = _alias_mrm_keys(df_mrm.filter(F.col(step.join_key).isNotNull()))

    join_cond = F.col(step.join_key) == F.col(f"_mrm_{step.join_key}")
    if step.extra_cond is not None:
        join_cond = join_cond & step.extra_cond()

    df_matched = (
        df_cpt_valid
        .join(df_mrm_valid, on=join_cond, how="inner")
        .transform(_drop_mrm_keys)
        .withColumn("TYPE_RECONCILIATION", F.lit(step.label))
    )

    _persist_count(df_matched)
    df_cpt_remaining, df_mrm_remaining = _exclude_matched(df_cpt, df_mrm, df_matched)
    return df_matched, df_cpt_remaining, df_mrm_remaining


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


# ============================================================================
# FILTRAGE DES CONSIGNES MRM
# ============================================================================

def filter_mrm_by_action(
    df_mrm        : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
) -> Tuple[DataFrame, DataFrame]:
    """
    Sépare les MRM selon leur consigne métier.

        MRM_DELETE                       → df_to_remove
        MRM_KEEP / STUDY / ADD / None    → df_to_process (conservés par défaut)

    Returns:
        Tuple (df_to_remove, df_to_process)
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

    # "!= MRM_DELETE" évalue NULL pour les MRM_ACTION nulles → Spark les exclut
    # silencieusement. On ajoute isNull explicite pour les conserver.
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
# DÉCLARATIONS TARDIVES (MRM N+1, N+2, …)
# ============================================================================

def recover_late_declarations(
    df_result  : DataFrame,
    inventories: List[Tuple[str, DataFrame]],
    key        : str = "key_no_date",
) -> DataFrame:
    """
    Donne une seconde chance aux CPT_ONLY sur des inventaires MRM ultérieurs.

    Cascade : chaque CPT_ONLY est testé contre les inventaires dans l'ordre fourni.
    Le premier inventaire qui contient la clé récupère le dossier :
        - TYPE_RECONCILIATION → "CPT_LATE"
        - LATE_SOURCE         → tag de l'inventaire trouveur (ex: "MRM_N1")
        - colonnes MRM_*      → enrichies par l'inventaire (PM, conclusion, …)
    Les non retrouvés restent CPT_ONLY.

    Clé : `key_no_date` (rpp + dob + garantie + nom). Chaque inventaire est
    dédoublonné sur la clé pour éviter un fan-out.

    Args:
        df_result   : DataFrame réconcilié (sortie de matching_waterfall)
        inventories : liste ORDONNÉE de (tag, df_mrm_clean)
        key         : colonne clé de rapprochement
    """
    is_cpt_only = F.col("TYPE_RECONCILIATION") == "CPT_ONLY"
    rest      = df_result.filter(~is_cpt_only)
    remaining = _persist_count(df_result.filter(is_cpt_only))

    recovered: List[DataFrame] = []
    for tag, df_mrm in inventories:
        mrm_enrich = F.broadcast(
            df_mrm.filter(F.col(key).isNotNull())
                  .select(key, *[c for c in df_mrm.columns if c.startswith("MRM_")])
                  .dropDuplicates([key])
        )
        remaining_cpt = remaining.select(*[c for c in remaining.columns if not c.startswith("MRM_")])
        hit = _persist_count(
            remaining_cpt.join(mrm_enrich, on=key, how="inner")
                         .withColumn("TYPE_RECONCILIATION", F.lit("CPT_LATE"))
                         .withColumn("LATE_SOURCE", F.lit(tag))
        )
        new_remaining = remaining_cpt.join(
            F.broadcast(hit.select(key).distinct()), on=key, how="left_anti"
        )
        remaining = _swap_persist(remaining, new_remaining)
        recovered.append(hit)

    return reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [rest, remaining, *recovered],
    )


# ============================================================================
# WATERFALL PRINCIPAL
# ============================================================================

def matching_waterfall(
    df_cpt_clean  : DataFrame,
    df_mrm_clean  : DataFrame,
    pre_steps     : Optional[List[WaterfallStep]] = None,
    post_steps    : Optional[List[WaterfallStep]] = None,
    conclusion_col: str = "MRM_CONCLUSION",
    ip_offset     : Optional[int] = IP_GARANTIE_OFFSET,
) -> DataFrame:
    """
    Cascade de réconciliation CPT/MRM complète.

    Ordre :
        1. pre_steps    — matchs stricts/élargis (défaut: STEPS_PRE_FILTER)
        2. Filtrage MRM — écarte les MRM_DELETE
        3. post_steps   — IP / rechute / date_retard (défaut: STEPS_POST_FILTER)
        4. Orphelins    — CPT_ONLY + MRM_MISSING

    Anti-régression : chaque ligne CPT (resp. MRM) reçoit un uid unique en
    entrée. Les étapes anti-joinent sur ces uids — un dossier matché à l'étape K
    est garanti exclu des étapes K+1..N même si la clé devient moins discriminante.

    Args:
        df_cpt_clean   : DataFrame CPT nettoyé et préfixé (CPT_*)
        df_mrm_clean   : DataFrame MRM nettoyé et préfixé (MRM_*)
        pre_steps      : Étapes pré-filtrage MRM (défaut: STEPS_PRE_FILTER)
        post_steps     : Étapes post-filtrage MRM (défaut: STEPS_POST_FILTER)
        conclusion_col : Colonne conclusion MRM
        ip_offset      : Si None, l'étape MATCH_IP est retirée des post_steps.
    """
    pre  = list(pre_steps  if pre_steps  is not None else STEPS_PRE_FILTER)
    post = list(post_steps if post_steps is not None else STEPS_POST_FILTER)
    if ip_offset is None:
        post = [s for s in post if s.label != "MATCH_IP"]

    print(f"\n[WATERFALL] pre  : {[s.label for s in pre]}")
    print(f"[WATERFALL] post : {[s.label for s in post]}")

    # monotonically_increasing_id() est non-déterministe → matérialiser pour
    # figer la valeur (sinon l'anti-join inter-étapes laisse fuiter des dossiers).
    df_cpt_remaining = _persist_count(df_cpt_clean.withColumn(_UID_CPT, F.monotonically_increasing_id()))
    df_mrm_remaining = _persist_count(df_mrm_clean.withColumn(_UID_MRM, F.monotonically_increasing_id()))

    results: List[DataFrame] = []

    def _run(steps: List[WaterfallStep]) -> None:
        nonlocal df_cpt_remaining, df_mrm_remaining
        for step in steps:
            df_matched, new_cpt, new_mrm = _execute_step(df_cpt_remaining, df_mrm_remaining, step)
            results.append(df_matched)
            df_cpt_remaining = _swap_persist(df_cpt_remaining, new_cpt)
            df_mrm_remaining = _swap_persist(df_mrm_remaining, new_mrm)

    _run(pre)

    # Filtrage consignes MRM (entre les deux phases).
    df_mrm_removed, new_mrm = filter_mrm_by_action(df_mrm_remaining, conclusion_col)
    results.append(df_mrm_removed)
    df_mrm_remaining = _swap_persist(df_mrm_remaining, new_mrm)

    _run(post)

    # Orphelins finaux.
    df_cpt_orphans, df_mrm_critiques = tag_orphans(df_cpt_remaining, df_mrm_remaining)
    results.extend([df_cpt_orphans, df_mrm_critiques])

    df_final = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), results)
    for c in (_UID_CPT, _UID_MRM):
        if c in df_final.columns:
            df_final = df_final.drop(c)

    print("[WATERFALL] union terminée — métriques calculées en aval")
    return df_final
