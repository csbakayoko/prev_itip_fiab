"""
Logique métier de réconciliation en cascade (waterfall).

Flux :
    CPT + MRM
        │
        ├─ Étape 1 : MATCH_EXACT         (key_strict)
        ├─ Étape 2 : MATCH_WINDOW        (key_no_date + fenêtre ±N jours)
        ├─ Étape 3 : MATCH_TRONC         (key_strict_tronc — troncature CPT 20 chars)
        ├─ Étape 4 : MATCH_TRONC_WINDOW  (key_no_date_tronc + fenêtre ±N jours)
        │   [Zone d'extension — WATERFALL_STEPS]
        ├─ Filtrage consignes MRM        (MRM_DELETE → écarté)
        └─ Orphelins                     (CPT_ONLY / MRM_MISSING)
            │
            └─ Union finale → DataFrame réconcilié (+ colonne CLAUSE)

Fonctions publiques :
    matching_waterfall()        — Cascade complète → df_result avec TYPE_RECONCILIATION
    categorize_mrm_conclusion() — Catégorise la conclusion MRM brute

Note : derive_clause_column / derive_invalidite_column sont dans core/enrich.py
(enrichissements post-waterfall, pas du matching pur).
"""

from dataclasses import dataclass
from functools import reduce
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from config import MATCH_LABELS, WINDOW_DAYS, IP_GARANTIE_OFFSET, RELAPSE_WINDOW_DAYS
from modules.transform import drop_duplicate_columns


# ============================================================================
# MODÈLE D'ÉTAPE DE MATCHING
# ============================================================================

@dataclass(frozen=True)
class WaterfallStep:
    """
    Définit une étape du waterfall.

    Deux modes :
        Égalité pure       : join_key seul (ex: MATCH_EXACT sur key_strict)
        Égalité + fenêtre  : join_key + condition |datediff(cpt_date, mrm_date)| <= max_days
                              (ex: MATCH_WINDOW sur key_no_date avec ±7 jours)

    Attributs :
        label      : valeur posée dans TYPE_RECONCILIATION (ex: "MATCH_EXACT")
        join_key   : nom de la colonne clé (partagée CPT/MRM après prefix_columns avec keep=)
        date_cpt   : Optional, colonne date CPT pour la fenêtre (ex: "CPT_D_SURVENANCE")
        date_mrm   : Optional, colonne date MRM pour la fenêtre (ex: "MRM_D_SURVENANCE")
        max_days   : tolérance en jours (lue depuis settings.WINDOW_DAYS par défaut)
    """
    label    : str
    join_key : str
    date_cpt : Optional[str] = None
    date_mrm : Optional[str] = None
    max_days : Optional[int] = None

    @property
    def is_windowed(self) -> bool:
        return self.date_cpt is not None and self.date_mrm is not None


# ============================================================================
# ÉTAPES DE MATCHING — DÉCLARATIF
# ============================================================================
#
# Cascade actuelle :
#   1. MATCH_EXACT  : égalité sur key_strict  (rpp + dob + survenance(jour) + garantie + NOM complet)
#   2. MATCH_WINDOW : égalité sur key_no_date (rpp + dob + garantie + NOM complet)
#                     + tolérance |CPT_D_SURVENANCE - MRM_D_SURVENANCE| <= WINDOW_DAYS jours.
#
# Pour ajouter une étape : ajouter un WaterfallStep ci-dessous.
# Variantes commentées disponibles pour activation après validation métier.
# ============================================================================

WATERFALL_STEPS: List[WaterfallStep] = [

    # ── Étape 1 : égalité stricte (date au jour, NOM_PRENOM complet) ──────────
    WaterfallStep(label="MATCH_EXACT", join_key="key_strict"),

    # ── Étape 2 : fenêtre ±N jours sur la date de survenance (nom complet) ────
    # ±7j est plus précis qu'un match au mois et capture les vrais décalages de
    # saisie (week-end, fin de mois, jour ouvré).
    WaterfallStep(
        label    = "MATCH_WINDOW",
        join_key = "key_no_date",
        date_cpt = "CPT_D_SURVENANCE",
        date_mrm = "MRM_D_SURVENANCE",
        max_days = WINDOW_DAYS,
    ),

    # ── Étape 3 : troncature CPT 20 chars (date jour) ─────────────────────────
    # CPT limite NOM_PRENOM à 20 caractères, la coupure tombe parfois dans le
    # dernier prénom (ex: "REICHENAUER CHRISTELLE" → "REICHENAUER CHRISTEL").
    # Les deux côtés appliquent LEFT(20) uppercase + strip espaces.
    WaterfallStep(label="MATCH_TRONC", join_key="key_strict_tronc"),

    # ── Étape 4 : troncature CPT 20 chars + fenêtre ±N jours ──────────────────
    # Absorbe les dossiers qui cumulent les deux anomalies (troncature + décalage date).
    WaterfallStep(
        label    = "MATCH_TRONC_WINDOW",
        join_key = "key_no_date_tronc",
        date_cpt = "CPT_D_SURVENANCE",
        date_mrm = "MRM_D_SURVENANCE",
        max_days = WINDOW_DAYS,
    ),
]


# Colonnes internes pour anti-join inter-étapes (uid = identifiant unique de ligne)
_UID_CPT = "_cpt_uid"
_UID_MRM = "_mrm_uid"


def _persist_count(df: DataFrame) -> DataFrame:
    """Matérialise df (persist + action) pour figer le plan et éviter les recomputations."""
    df.persist()
    df.count()
    return df


def _swap_persist(old: DataFrame, new: DataFrame) -> DataFrame:
    """Persiste `new`, libère `old` — utilisé entre étapes du waterfall."""
    _persist_count(new)
    try:
        old.unpersist()
    except Exception:
        pass
    return new


# ============================================================================
# ÉTAPE DE MATCHING GÉNÉRIQUE
# ============================================================================

def execute_matching_step(
    df_cpt: DataFrame,
    df_mrm: DataFrame,
    step  : WaterfallStep,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Exécute une étape de matching — égalité pure ou égalité + fenêtre date.

    Anti-join : basé sur les uids (_cpt_uid / _mrm_uid) attribués en amont par
    matching_waterfall(). Garantit qu'un dossier déjà matché à l'étape N ne
    pourra pas l'être à l'étape N+1, même si la clé est moins discriminante
    (cas du windowed matching où key_no_date est partagée par plusieurs lignes).

    Args:
        df_cpt : DataFrame CPT restant (annoté _cpt_uid)
        df_mrm : DataFrame MRM restant (annoté _mrm_uid)
        step   : WaterfallStep décrivant l'étape

    Returns:
        Tuple (df_matched, df_cpt_remaining, df_mrm_remaining)
    """
    for side, df in [("CPT", df_cpt), ("MRM", df_mrm)]:
        if step.join_key not in df.columns:
            raise ValueError(f"Colonne '{step.join_key}' absente du DataFrame {side}.")

    # Exclure les clés nulles (null ne doit jamais matcher).
    df_cpt_valid = df_cpt.filter(F.col(step.join_key).isNotNull())
    df_mrm_valid = df_mrm.filter(F.col(step.join_key).isNotNull())

    if step.is_windowed:
        # Égalité sur join_key + |datediff(cpt_date, mrm_date)| <= max_days
        if step.date_cpt not in df_cpt.columns or step.date_mrm not in df_mrm.columns:
            raise ValueError(
                f"Colonnes date '{step.date_cpt}'/'{step.date_mrm}' absentes "
                f"pour l'étape windowed '{step.label}'."
            )
        join_cond = (
            (F.col(step.join_key) == F.col(f"_mrm_{step.join_key}"))
            & (
                F.abs(F.datediff(F.col(step.date_cpt), F.col(step.date_mrm)))
                <= int(step.max_days or WINDOW_DAYS)
            )
            & F.col(step.date_cpt).isNotNull()
            & F.col(step.date_mrm).isNotNull()
        )
        # Alias la clé côté MRM pour éviter l'ambiguïté Spark
        df_mrm_aliased = df_mrm_valid.withColumnRenamed(step.join_key, f"_mrm_{step.join_key}")
        df_matched = (
            df_cpt_valid
            .join(df_mrm_aliased, on=join_cond, how="inner")
            .drop(f"_mrm_{step.join_key}")
            .withColumn("TYPE_RECONCILIATION", F.lit(step.label))
            .transform(drop_duplicate_columns)
        )
    else:
        # Égalité pure sur join_key (cas MATCH_EXACT)
        df_matched = (
            df_cpt_valid
            .join(df_mrm_valid, on=step.join_key, how="inner")
            .withColumn("TYPE_RECONCILIATION", F.lit(step.label))
            .transform(drop_duplicate_columns)
        )

    # ── Matérialiser df_matched : il sera lu 3 fois (2 anti-joins + union finale).
    _persist_count(df_matched)

    # ── Anti-join basé sur les uids — robuste pour les clés non-uniques.
    # Broadcast : les uids matchés sont par construction petits (~quelques k lignes).
    matched_cpt_uids = F.broadcast(df_matched.select(_UID_CPT).distinct())
    matched_mrm_uids = F.broadcast(df_matched.select(_UID_MRM).distinct())
    df_cpt_remaining = df_cpt.join(matched_cpt_uids, on=_UID_CPT, how="left_anti")
    df_mrm_remaining = df_mrm.join(matched_mrm_uids, on=_UID_MRM, how="left_anti")

    return df_matched, df_cpt_remaining, df_mrm_remaining


# ============================================================================
# ÉTAPE PASSAGE IT → IP
# ============================================================================

def execute_ip_step(
    df_cpt      : DataFrame,
    df_mrm      : DataFrame,
    offset      : int,
    key         : str = "key_no_garantie",
    garantie_cpt: str = "CPT_GARANTIE",
    garantie_mrm: str = "MRM_GARANTIE",
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Récupère les orphelins issus d'un passage incapacité (IT) → invalidité (IP).

    Principe :
        1. Jointure sur `key` = rpp + dob + survenance(jour) + nom (SANS garantie).
        2. Garde-fou : ne valide que si garantie_CPT − garantie_MRM == offset
           (le passage IT → IP décale le code garantie d'un offset constant).
           Les codes garantie non numériques → cast null → écart null → écartés.
        3. Les paires validées sont taguées MATCH_IP ; les autres restent orphelines.

    Anti-join sur les uids (_cpt_uid / _mrm_uid posés par matching_waterfall),
    comme execute_matching_step.

    Args:
        df_cpt       : DataFrame CPT orphelin (annoté _cpt_uid)
        df_mrm       : DataFrame MRM orphelin (annoté _mrm_uid)
        offset        : écart de code garantie attendu (CPT − MRM)
        key          : clé de jointure sans garantie
        garantie_cpt : colonne code garantie côté CPT
        garantie_mrm : colonne code garantie côté MRM

    Returns:
        Tuple (df_ip_matched, df_cpt_remaining, df_mrm_remaining)
    """
    df_cpt_valid = df_cpt.filter(F.col(key).isNotNull())
    df_mrm_valid = df_mrm.filter(F.col(key).isNotNull()).withColumnRenamed(key, f"_mrm_{key}")

    diff = F.abs(F.col(garantie_cpt).cast("int") - F.col(garantie_mrm).cast("int"))
    join_cond = (F.col(key) == F.col(f"_mrm_{key}")) & (diff == F.lit(offset))

    df_matched = (
        df_cpt_valid
        .join(df_mrm_valid, on=join_cond, how="inner")
        .drop(f"_mrm_{key}")
        .withColumn("TYPE_RECONCILIATION", F.lit("MATCH_IP"))
        .transform(drop_duplicate_columns)
    )

    _persist_count(df_matched)
    matched_cpt_uids = F.broadcast(df_matched.select(_UID_CPT).distinct())
    matched_mrm_uids = F.broadcast(df_matched.select(_UID_MRM).distinct())
    df_cpt_remaining = df_cpt.join(matched_cpt_uids, on=_UID_CPT, how="left_anti")
    df_mrm_remaining = df_mrm.join(matched_mrm_uids, on=_UID_MRM, how="left_anti")

    return df_matched, df_cpt_remaining, df_mrm_remaining


# ============================================================================
# ÉTAPE RECHUTE IT
# ============================================================================

def execute_rechute_step(
    df_cpt       : DataFrame,
    df_mrm       : DataFrame,
    relapse_days : int  = RELAPSE_WINDOW_DAYS,
    key          : str  = "key_no_date_no_garantie",
    garantie_cpt : str  = "CPT_GARANTIE",
    garantie_mrm : str  = "MRM_GARANTIE",
    date_cpt     : str  = "CPT_D_SURVENANCE",
    date_mrm     : str  = "MRM_D_SURVENANCE",
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Récupère les orphelins issus d'une rechute IT.

    Principe :
        1. Jointure sur `key` = rpp + dob + nom (SANS date, SANS garantie) —
           la clé la plus large pour attraper la rechute.
        2. Garde-fou 1 : garantie identique des deux côtés
           (la rechute IT reste sur le même code garantie).
        3. Garde-fou 2 : |datediff(CPT_D_SURVENANCE, MRM_D_SURVENANCE)| doit être
           strictement positif et ≤ relapse_days.
           > 0  → les deux dates diffèrent (pas un doublon de survenance exacte,
                   déjà absorbé par les étapes précédentes).
           ≤ N  → écart dans la fenêtre réglementaire (défaut : 30 jours).
           La valeur absolue est utilisée — peu importe quel côté est antérieur.

    Anti-join sur les uids (_cpt_uid / _mrm_uid posés par matching_waterfall).

    Args:
        df_cpt        : DataFrame CPT orphelin (annoté _cpt_uid)
        df_mrm        : DataFrame MRM orphelin (annoté _mrm_uid)
        relapse_days  : fenêtre max en jours (défaut : RELAPSE_WINDOW_DAYS)
        key           : clé de jointure sans date ni garantie
        garantie_cpt  : colonne code garantie côté CPT
        garantie_mrm  : colonne code garantie côté MRM
        date_cpt      : colonne date de survenance côté CPT
        date_mrm      : colonne date de survenance côté MRM

    Returns:
        Tuple (df_rechute_matched, df_cpt_remaining, df_mrm_remaining)
    """
    df_cpt_valid = df_cpt.filter(F.col(key).isNotNull())
    df_mrm_valid = df_mrm.filter(F.col(key).isNotNull()).withColumnRenamed(key, f"_mrm_{key}")

    ecart = F.abs(F.datediff(F.col(date_cpt), F.col(date_mrm)))  # |CPT − MRM| en jours

    join_cond = (
        (F.col(key) == F.col(f"_mrm_{key}"))
        & (F.col(garantie_cpt).cast("int") == F.col(garantie_mrm).cast("int"))
        & (ecart > 0)
        & (ecart <= F.lit(relapse_days))
        & F.col(date_cpt).isNotNull()
        & F.col(date_mrm).isNotNull()
        & F.col(garantie_cpt).isNotNull()
        & F.col(garantie_mrm).isNotNull()
    )

    df_matched = (
        df_cpt_valid
        .join(df_mrm_valid, on=join_cond, how="inner")
        .drop(f"_mrm_{key}")
        .withColumn("TYPE_RECONCILIATION", F.lit("MATCH_RECHUTE"))
        .transform(drop_duplicate_columns)
    )

    _persist_count(df_matched)
    matched_cpt_uids = F.broadcast(df_matched.select(_UID_CPT).distinct())
    matched_mrm_uids = F.broadcast(df_matched.select(_UID_MRM).distinct())
    df_cpt_remaining = df_cpt.join(matched_cpt_uids, on=_UID_CPT, how="left_anti")
    df_mrm_remaining = df_mrm.join(matched_mrm_uids, on=_UID_MRM, how="left_anti")

    return df_matched, df_cpt_remaining, df_mrm_remaining


# ============================================================================
# ÉTAPE DATE EN RETARD MRM
# ============================================================================

def execute_date_retard_step(
    df_cpt   : DataFrame,
    df_mrm   : DataFrame,
    min_days : int = WINDOW_DAYS,
    key      : str = "key_no_date",
    date_cpt : str = "CPT_D_SURVENANCE",
    date_mrm : str = "MRM_D_SURVENANCE",
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Capture les orphelins où CPT et MRM désignent le même sinistre mais avec
    des dates de survenance décalées — CPT toujours plus récent que MRM.

    Origine observée : CPT enregistre la date révisée (prolongation, rechute
    intégrée), MRM conserve la date d'origine. L'écart peut atteindre 6 mois.
    Ces dossiers ne peuvent pas être tranchés métier sans investigation :
    ils sont donc capturés et tagués MATCH_DATE_RETARD pour analyse séparée,
    sans être comptabilisés comme matchés définitifs.

    Condition :
        datediff(CPT_D_SURVENANCE, MRM_D_SURVENANCE) > min_days
        → CPT est strictement postérieur à MRM, au-delà de la fenêtre normale
          déjà couverte par MATCH_WINDOW (±WINDOW_DAYS).

    Clé : key_no_date (rpp + dob + garantie + nom, sans date) — identique à
    MATCH_WINDOW, mais sans borne supérieure sur l'écart.

    Args:
        df_cpt   : DataFrame CPT orphelin (annoté _cpt_uid)
        df_mrm   : DataFrame MRM orphelin (annoté _mrm_uid)
        min_days : borne basse exclusive — évite le chevauchement avec MATCH_WINDOW
        key      : clé de jointure sans date
        date_cpt : colonne date de survenance CPT
        date_mrm : colonne date de survenance MRM

    Returns:
        Tuple (df_date_retard, df_cpt_remaining, df_mrm_remaining)
    """
    df_cpt_valid = df_cpt.filter(F.col(key).isNotNull())
    df_mrm_valid = (
        df_mrm.filter(F.col(key).isNotNull())
              .withColumnRenamed(key, f"_mrm_{key}")
    )

    ecart = F.datediff(F.col(date_cpt), F.col(date_mrm))   # CPT − MRM (toujours > 0)

    join_cond = (
        (F.col(key) == F.col(f"_mrm_{key}"))
        & (ecart > F.lit(min_days))
        & F.col(date_cpt).isNotNull()
        & F.col(date_mrm).isNotNull()
    )

    df_matched = (
        df_cpt_valid
        .join(df_mrm_valid, on=join_cond, how="inner")
        .drop(f"_mrm_{key}")
        .withColumn("TYPE_RECONCILIATION", F.lit("MATCH_DATE_RETARD"))
        .transform(drop_duplicate_columns)
    )

    _persist_count(df_matched)
    matched_cpt_uids = F.broadcast(df_matched.select(_UID_CPT).distinct())
    matched_mrm_uids = F.broadcast(df_matched.select(_UID_MRM).distinct())
    df_cpt_remaining = df_cpt.join(matched_cpt_uids, on=_UID_CPT, how="left_anti")
    df_mrm_remaining = df_mrm.join(matched_mrm_uids, on=_UID_MRM, how="left_anti")

    return df_matched, df_cpt_remaining, df_mrm_remaining


# ============================================================================
# CATÉGORISATION DES CONSIGNES MRM
# ============================================================================

def categorize_mrm_conclusion(col: F.Column) -> F.Column:
    """
    Catégorise la conclusion MRM selon les consignes métier officielles.

    Retient :
        MRM_KEEP   → PM MRM à conserver

    Retient, mais étude complémentaire :
        MRM_ADD / MRM_STUDY  → PM à ajouter / à étudier

    Écarte :
        MRM_DELETE → PM MRM à supprimer

    Args:
        col : Colonne Spark contenant la conclusion MRM (texte libre)

    Returns:
        Colonne Spark avec le label catégoriel, None si non reconnu
    """
    text = F.lower(F.trim(col))

    return (
        # --- À conserver ---
        F.when(text.contains("pm mrm à conserver"),                    "MRM_KEEP")

        # --- À ajouter ---
        .when(text.contains("pm à ajouter"),                           "MRM_ADD")
        .when(text.contains("pm dont l'ajout est à étudier"),          "MRM_ADD")

        # --- À étudier / ajouter ---
        .when(text.contains("pm mrm à étudier"),                       "MRM_STUDY")

        # --- À supprimer (cas composé en premier) ---
        .when(text.contains("psap à conserver et pm mrm à supprimer"), "MRM_DELETE")
        .when(text.contains("pm mrm à supprimer"),                     "MRM_DELETE")

        # --- Non reconnu ---
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
    Sépare les dossiers MRM selon leur consigne métier.

        MRM_DELETE            → écartés (rouge dans le rapport)
        MRM_KEEP / MRM_STUDY / MRM_ADD  → conservés pour le matching
        None (non reconnu)    → conservés par défaut (prudence métier)

    Args:
        df_mrm         : DataFrame MRM résiduel post-matching
        conclusion_col : Colonne contenant la conclusion MRM

    Returns:
        Tuple (df_to_remove, df_to_process)
    """
    if conclusion_col not in df_mrm.columns:
        raise ValueError(f"Colonne '{conclusion_col}' absente du DataFrame MRM.")

    df_categorized = df_mrm.withColumn(
        "MRM_ACTION",
        categorize_mrm_conclusion(F.col(conclusion_col))
    )

    is_delete = F.col("MRM_ACTION") == "MRM_DELETE"

    df_to_remove = (
        df_categorized
        .filter(is_delete)
        .withColumn("TYPE_RECONCILIATION", F.lit("MRM_DELETE"))
        .drop("MRM_ACTION")
        .transform(drop_duplicate_columns)
    )

    # Fix T-03 : "!= MRM_DELETE" évalue NULL pour les MRM_ACTION nulles → Spark
    # les exclut silencieusement. On ajoute explicitement isNull pour les conserver
    # (KEEP + STUDY + ADD + conclusion non reconnue).
    df_to_process = (
        df_categorized
        .filter(~is_delete | F.col("MRM_ACTION").isNull())
        .drop("MRM_ACTION")
        .transform(drop_duplicate_columns)
    )

    return df_to_remove, df_to_process


# ============================================================================
# ORPHELINS FINAUX
# ============================================================================

def tag_orphans(
    df_cpt: DataFrame,
    df_mrm: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """
    Tague les dossiers non réconciliés après toutes les étapes de matching.

        CPT_ONLY    → présent dans CPT, absent dans MRM
        MRM_MISSING → présent dans MRM, absent dans CPT

    Args:
        df_cpt : DataFrame CPT résiduel (non matché)
        df_mrm : DataFrame MRM résiduel (non matché, non supprimé)

    Returns:
        Tuple (df_cpt_orphans, df_mrm_critiques)
    """
    df_cpt_orphans = (
        df_cpt
        .withColumn("TYPE_RECONCILIATION", F.lit("CPT_ONLY"))
        .transform(drop_duplicate_columns)
    )

    df_mrm_critiques = (
        df_mrm
        .withColumn("TYPE_RECONCILIATION", F.lit("MRM_MISSING"))
        .transform(drop_duplicate_columns)
    )

    return df_cpt_orphans, df_mrm_critiques


# ============================================================================
# DÉCLARATIONS TARDIVES (MRM N+1, N+2, …)
# ============================================================================

def recover_late_declarations(
    df_result  : DataFrame,
    inventories: List[Tuple[str, DataFrame]],
    key        : str = "key_no_date",
) -> DataFrame:
    """
    Donne une seconde (puis troisième…) chance aux CPT_ONLY sur des inventaires
    MRM ultérieurs (N+1, N+2…).

    Un dossier présent dans le COMPTE mais absent du MRM courant, puis retrouvé
    dans un MRM postérieur, est une **déclaration tardive** (le sinistre a été
    intégré côté MRM après la date d'inventaire courante).

    Cascade : chaque CPT_ONLY est testé contre les inventaires DANS L'ORDRE
    fourni. Le premier inventaire qui contient la clé récupère le dossier ;
    les CPT_ONLY restants retentent leur chance sur l'inventaire suivant.
    Un dossier récupéré :
        - TYPE_RECONCILIATION → "CPT_LATE"
        - LATE_SOURCE         → tag de l'inventaire trouveur (ex: "MRM_N1")
        - colonnes MRM_*      → ENRICHIES avec celles de l'inventaire (PM,
                                conclusion, etc.) → exploitables dans les analyses.
    Les CPT_ONLY non retrouvés dans aucun inventaire restent CPT_ONLY (définitifs).

    Clé : `key_no_date` (rpp + dob + garantie + nom, sans date de survenance) —
    tolère un décalage de saisie entre millésimes. Chaque inventaire est
    dédoublonné sur la clé (1 ligne MRM par clé) pour éviter toute fan-out.

    Args:
        df_result   : DataFrame réconcilié (sortie de matching_waterfall)
        inventories : liste ORDONNÉE de (tag, df_mrm_clean), ex:
                      [("MRM_N1", mrm_n1), ("MRM_N2", mrm_n2)]
        key         : colonne clé de rapprochement

    Returns:
        df_result avec les CPT_LATE enrichis (MRM_*) + colonne LATE_SOURCE.
    """
    is_cpt_only = F.col("TYPE_RECONCILIATION") == "CPT_ONLY"
    rest      = df_result.filter(~is_cpt_only)
    remaining = _persist_count(df_result.filter(is_cpt_only))

    recovered: List[DataFrame] = []
    for tag, df_mrm in inventories:
        # 1 ligne MRM par clé → pas de fan-out (duplication d'un CPT récupéré).
        # Broadcast : un inventaire MRM dédoublonné sur la clé est petit.
        mrm_enrich = F.broadcast(
            df_mrm.filter(F.col(key).isNotNull())
                  .select(key, *[c for c in df_mrm.columns if c.startswith("MRM_")])
                  .dropDuplicates([key])
        )
        # On repart des seules colonnes non-MRM (les MRM_* des CPT_ONLY sont nulles,
        # on les laisse l'inventaire trouveur les remplir).
        remaining_cpt = remaining.select(
            *[c for c in remaining.columns if not c.startswith("MRM_")]
        )
        hit = _persist_count(
            remaining_cpt.join(mrm_enrich, on=key, how="inner")
                         .withColumn("TYPE_RECONCILIATION", F.lit("CPT_LATE"))
                         .withColumn("LATE_SOURCE", F.lit(tag))
                         .transform(drop_duplicate_columns)
        )
        new_remaining = remaining_cpt.join(
            F.broadcast(hit.select(key).distinct()), on=key, how="left_anti"
        )
        remaining = _swap_persist(remaining, new_remaining)
        recovered.append(hit)

    # rest + CPT_ONLY définitifs (sans MRM_*) + CPT_LATE enrichis
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
    steps         : Optional[List[WaterfallStep]] = None,
    conclusion_col: str = "MRM_CONCLUSION",
    ip_offset     : Optional[int] = IP_GARANTIE_OFFSET,
) -> DataFrame:
    """
    Cascade de réconciliation CPT/MRM complète.

    Ordre :
        1..N  Match steps   — WaterfallStep dans WATERFALL_STEPS
        N+1   Filtrage MRM  — écarte les MRM_DELETE
        N+2   Orphelins     — CPT_ONLY + MRM_MISSING

    Anti-régression : chaque ligne CPT (resp. MRM) reçoit un uid unique
    en entrée. Les étapes suivantes utilisent ces uids pour anti-joiner
    (et non la clé) — un dossier matché à l'étape K est garanti exclus
    des étapes K+1..N même si la clé devient moins discriminante (cas
    du windowed matching).

    Args:
        df_cpt_clean   : DataFrame CPT nettoyé et préfixé (CPT_*)
        df_mrm_clean   : DataFrame MRM nettoyé et préfixé (MRM_*)
        steps          : Étapes custom (défaut: WATERFALL_STEPS)
        conclusion_col : Colonne conclusion MRM

    Returns:
        DataFrame unifié avec TYPE_RECONCILIATION sur chaque dossier
        (les colonnes internes _cpt_uid / _mrm_uid sont supprimées en fin).
    """
    active_steps = steps or WATERFALL_STEPS
    print(f"\n[WATERFALL] étapes actives : {[s.label for s in active_steps]}")

    # Attribution d'un uid unique par ligne. monotonically_increasing_id() est
    # NON-DÉTERMINISTE : Spark le recalcule à chaque évaluation lazy. Comme ces uid
    # servent de clé d'anti-join inter-étapes, on DOIT figer leurs valeurs en
    # matérialisant les frames (persist + action) — sinon l'anti-join compare des
    # uid recalculés différemment et laisse fuiter des dossiers déjà matchés.
    df_cpt_remaining = df_cpt_clean.withColumn(_UID_CPT, F.monotonically_increasing_id()).persist()
    df_mrm_remaining = df_mrm_clean.withColumn(_UID_MRM, F.monotonically_increasing_id()).persist()
    df_cpt_remaining.count()
    df_mrm_remaining.count()

    results: List[DataFrame] = []

    # ── Étapes de matching ──────────────────────────────────────────
    # À chaque tour : on persiste les nouveaux remaining et on libère les anciens.
    # Sans ça, le plan logique des anti-joins s'empile sur 7 étapes et explose
    # le driver (cluster détaché).
    for step in active_steps:
        df_matched, new_cpt, new_mrm = execute_matching_step(
            df_cpt_remaining, df_mrm_remaining, step=step,
        )
        results.append(df_matched)
        df_cpt_remaining = _swap_persist(df_cpt_remaining, new_cpt)
        df_mrm_remaining = _swap_persist(df_mrm_remaining, new_mrm)

    # ── Filtrage consignes MRM ──────────────────────────────────────
    df_mrm_removed, new_mrm = filter_mrm_by_action(
        df_mrm_remaining, conclusion_col=conclusion_col
    )
    results.append(df_mrm_removed)
    df_mrm_remaining = _swap_persist(df_mrm_remaining, new_mrm)

    # ── Passage IT → IP (sur les orphelins, hors MRM_DELETE) ────────
    if ip_offset is not None:
        print(f"[WATERFALL] étape passage IP : offset garantie = {ip_offset}")
        df_ip_matched, new_cpt, new_mrm = execute_ip_step(
            df_cpt_remaining, df_mrm_remaining, offset=ip_offset,
        )
        results.append(df_ip_matched)
        df_cpt_remaining = _swap_persist(df_cpt_remaining, new_cpt)
        df_mrm_remaining = _swap_persist(df_mrm_remaining, new_mrm)

    # ── Rechute IT (sur les orphelins restants) ─────────────────────
    print(f"[WATERFALL] étape rechute IT : fenêtre = |datediff| ∈ ]0, {RELAPSE_WINDOW_DAYS}j]")
    df_rechute, new_cpt, new_mrm = execute_rechute_step(
        df_cpt_remaining, df_mrm_remaining,
    )
    results.append(df_rechute)
    df_cpt_remaining = _swap_persist(df_cpt_remaining, new_cpt)
    df_mrm_remaining = _swap_persist(df_mrm_remaining, new_mrm)

    # ── Date MRM en retard (CPT toujours plus récent, écart > WINDOW_DAYS) ──
    print(f"[WATERFALL] étape date retard MRM : datediff(CPT, MRM) > {WINDOW_DAYS}j")
    df_date_retard, new_cpt, new_mrm = execute_date_retard_step(
        df_cpt_remaining, df_mrm_remaining,
    )
    results.append(df_date_retard)
    df_cpt_remaining = _swap_persist(df_cpt_remaining, new_cpt)
    df_mrm_remaining = _swap_persist(df_mrm_remaining, new_mrm)

    # ── Orphelins finaux ────────────────────────────────────────────
    df_cpt_orphans, df_mrm_critiques = tag_orphans(df_cpt_remaining, df_mrm_remaining)
    results.append(df_cpt_orphans)
    results.append(df_mrm_critiques)

    # ── Guard union ─────────────────────────────────────────────────
    if not results:
        raise RuntimeError("Waterfall : aucun résultat produit — vérifier les étapes et les données.")

    # ── Union finale ────────────────────────────────────────────────
    df_final = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        results,
    )

    # Drop des uids internes (n'ont pas vocation à descendre dans Power BI)
    for c in (_UID_CPT, _UID_MRM):
        if c in df_final.columns:
            df_final = df_final.drop(c)

    print("[WATERFALL] union terminée — métriques calculées en aval")
    return df_final
