"""
Briques du matching — clés, conditions réutilisables, étape de join.

Architecture d'une étape (execute_matching_step) :
- join classique sur la clé courante, MRM dédoublonné sur la clé avant le
  join (un CPT matche au plus 1 MRM, pas de fan-out) et broadcasté ;
- anti-join sur la clé : les lignes matchées sortent du remaining ;
- _materialize() tronque la lignée à chaque étape (checkpoint fiable DBFS si
  CHECKPOINT_DIR est configuré) — indispensable avec l'autoscaling.

RECOVERY_KEYS = la cascade du repêchage (waterfall rejoué du plus strict au
plus flexible), consommée par core.match.recovery.
"""

from typing import Callable, Optional, Tuple

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from config import (
    WINDOW_DAYS, IP_GARANTIE_OFFSET, RELAPSE_WINDOW_DAYS, LOG_VOLUMETRIE,
)


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
    # Clés de secours « clause » (RPP remplacé par le n° de clause).
    "key_clause_strict",
    "key_clause_no_date",
    "key_clause_strict_tronc",
    "key_clause_no_date_tronc",
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


# Cascade du repêchage (recover_late_declarations) : REJOUE le waterfall
# principal dans le MÊME ORDRE (du plus strict au plus flexible). L'ordre
# n'est pas qu'une question de couverture mais de QUALITÉ d'appariement : un
# assuré à plusieurs sinistres (plusieurs survenances) partage la même clé
# sans date — le dropDuplicates sur une clé lâche choisirait une contrepartie
# arbitraire, alors que la clé stricte rapproche le bon dossier (bonne date,
# bon n° de sinistre) avant que les clés flexibles ne ratissent le reste.
# Mêmes règles et mêmes fenêtres que le waterfall : pas d'étape sans
# contrainte de date (un écart de survenance au-delà des fenêtres n'est pas
# considéré comme la même observation).
# Élément : (label LATE_KEY, clé de join, condition supplémentaire ou None).
RECOVERY_KEYS: Tuple = (
    ("MATCH_EXACT",         "key_strict",        None),
    ("MATCH_WINDOW",        "key_no_date",
     _windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS)),
    ("MATCH_TRONC",         "key_strict_tronc",  None),
    ("MATCH_TRONC_WINDOW",  "key_no_date_tronc",
     _windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS)),
) + (
    (("MATCH_IP",           "key_no_garantie",   _ip_cond(IP_GARANTIE_OFFSET)),)
    if IP_GARANTIE_OFFSET is not None else ()
) + (
    ("MATCH_RECHUTE",       "key_no_date",       _rechute_cond(RELAPSE_WINDOW_DAYS)),
    ("MATCH_RECHUTE_TRONC", "key_no_date_tronc", _rechute_cond(RELAPSE_WINDOW_DAYS)),
) + (
    # Clés de secours « clause » EN DERNIER : ne se déclenchent que sur le résidu
    # que les clés RPP n'ont pas rattrapé (RPP nul / mal renseigné). Mêmes
    # variantes que le pré-filtre (strict / fenêtre × nom complet / tronqué).
    ("MATCH_CLAUSE",              "key_clause_strict",        None),
    ("MATCH_CLAUSE_WINDOW",       "key_clause_no_date",
     _windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS)),
    ("MATCH_CLAUSE_TRONC",        "key_clause_strict_tronc",  None),
    ("MATCH_CLAUSE_TRONC_WINDOW", "key_clause_no_date_tronc",
     _windowed("CPT_D_SURVENANCE", "MRM_D_SURVENANCE", WINDOW_DAYS)),
)


# ============================================================================
# MATÉRIALISATION (troncature de lignée tolérante aux pertes d'executors)
# ============================================================================

def _materialize(df: DataFrame) -> DataFrame:
    """Matérialise le DataFrame et TRONQUE LA LIGNÉE.

    Si un checkpointDir est configuré (cf. CHECKPOINT_DIR dans profile.py) :
    checkpoint FIABLE — les partitions sont écrites sur DBFS et survivent à la
    perte d'un executor (autoscaling / nœuds spot Databricks).

    Sinon : localCheckpoint — les blocs restent dans la mémoire des executors ;
    un executor rendu au cluster ⇒ CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND
    irrécupérable (la lignée tronquée interdit le recalcul). À réserver aux
    clusters à taille fixe.
    """
    sc = df.sparkSession.sparkContext
    if sc.getCheckpointDir():
        return df.checkpoint(eager=True)
    return df.localCheckpoint(eager=True)


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

    Les remaining sont matérialisés via _materialize() pour TRONQUER LA LIGNÉE.
    Attention : cache()/persist() ne suffit PAS — un InMemoryRelation expose son
    plan caché comme "inner child", donc la sérialisation du plan
    (AdaptiveSparkPlanExec.onUpdatePlan → explainStringLocal) ré-expose toute la
    chaîne précédente et le plan-string explose quand même (OutOfMemoryError
    driver). Le checkpoint matérialise puis remplace la lignée par une feuille
    RDD opaque, sans plan interne → le plan reste plat à chaque étape.
    Checkpoint FIABLE (DBFS) si CHECKPOINT_DIR est configuré — indispensable
    avec l'autoscaling (un localCheckpoint perd ses blocs avec l'executor).
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
    df_matched = _materialize(
        df_matched.withColumn("TYPE_RECONCILIATION", F.lit(label))
    )
    # Comptage PUREMENT informatif (df_matched est déjà matérialisé) → gaté.
    if LOG_VOLUMETRIE:
        print(f"[matching]   ↳ {label} : {df_matched.count():,} matchs")

    # Anti-join sur la clé : on retire des remaining les lignes dont la clé est
    # dans matched. _materialize coupe la lignée (cf. docstring).
    matched_keys = F.broadcast(df_matched.select(key).distinct())
    df_cpt_rem = _materialize(df_cpt.join(matched_keys, on=key, how="left_anti"))
    df_mrm_rem = _materialize(df_mrm.join(matched_keys, on=key, how="left_anti"))

    return df_matched, df_cpt_rem, df_mrm_rem
