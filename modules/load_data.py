"""
Chargement des données brutes CPT et MRM.

Filtres symétriques (définis dans settings.py) :
    CLIENT_CLAUSES      — numéros de clause (sans préfixe), appliqués aux deux sources
    CLIENT_TYPE_CLAUSES — types de compte ("PB"/"HPB"), appliqués aux deux sources

Logique de traduction vers les formats natifs :
    CPT : clause = "CPB_<numéro>"  (préfixe encode le type de compte)
    MRM : n_clause_ratta1 = "<numéro>"  +  TYPE_CLAUSE = "PB" pour PB

Le CSV MRM est lu en mode `inferSchema=false` (fix T-02) — toutes les colonnes
arrivent en string, puis clean_mrm() applique les casts ciblés (to_date sur les
dates, regexp_replace+cast double sur les montants au format européen "12,34").
"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
import logging

from config import (
    DatabaseConfig,
    RUN_PARAMS,
    CLIENT_CPT_VISION,
    CLIENT_MRM_STATUT_INV,
    CLIENT_CLAUSES,
    CLIENT_TYPE_CLAUSES,
    TYPE_CLAUSE_CPT_PREFIX,
    TYPE_CLAUSE_MRM_VALUE,
    MRM_TYPE_CLAUSE_COL,
    DEV_MODE,
)
from modules._timing import timed_fn

logger = logging.getLogger(__name__)


# ============================================================================
# CHARGEMENT CPT
# ============================================================================

@timed_fn("load_cpt_raw")
def load_cpt_raw(spark: SparkSession, cfg: DatabaseConfig) -> DataFrame:
    """
    Charge les données CPT et applique les filtres en une seule passe.

    Filtres appliqués dans l'ordre :
        1. Vision  — obligatoire (toujours)
        2. Clause  — si CLIENT_CLAUSES défini :
                       construit les valeurs CPT = "<préfixe_type><numéro>"
                       pour chaque type × clause demandés.
        3. Type    — si CLIENT_TYPE_CLAUSES défini SANS filtre clause :
                       filtre sur le préfixe de la colonne `clause`.
                       (redondant quand le filtre clause est déjà actif)

    Exemples :
        CLIENT_CLAUSES = ["121981"], CLIENT_TYPE_CLAUSES = ["PB"]
            → filtre clause IN ["CPB_121981"]
        CLIENT_CLAUSES = None, CLIENT_TYPE_CLAUSES = ["PB"]
            → filtre clause LIKE "CPB_%"
        CLIENT_CLAUSES = None, CLIENT_TYPE_CLAUSES = None
            → pas de filtre clause

    Args:
        spark : SparkSession active
        cfg   : DatabaseConfig

    Returns:
        DataFrame CPT filtré, prêt pour le nettoyage
    """
    df = spark.table(cfg.cpt_table).filter(F.col("vision") == CLIENT_CPT_VISION)

    if CLIENT_CLAUSES:
        # Construire la liste des valeurs CPT complètes = préfixe + numéro
        # Types actifs : CLIENT_TYPE_CLAUSES si défini, sinon tous les types connus
        active_types = CLIENT_TYPE_CLAUSES or list(TYPE_CLAUSE_CPT_PREFIX.keys())
        cpt_values = [
            f"{TYPE_CLAUSE_CPT_PREFIX[t]}{num}"
            for t in active_types if t in TYPE_CLAUSE_CPT_PREFIX
            for num in CLIENT_CLAUSES
        ]
        if cpt_values:
            df = df.filter(F.col("clause").isin(cpt_values))

    elif CLIENT_TYPE_CLAUSES:
        # Pas de filtre clause, mais filtre sur le type (préfixe)
        prefixes = [TYPE_CLAUSE_CPT_PREFIX[t] for t in CLIENT_TYPE_CLAUSES if t in TYPE_CLAUSE_CPT_PREFIX]
        if prefixes:
            type_cond = reduce(
                lambda a, b: a | b,
                [F.col("clause").startswith(p) for p in prefixes],
            )
            df = df.filter(type_cond)

    # Pas de .count() ici — comptage centralisé dans api.py (un seul appel),
    # car .count() ici déclenche un job Spark complet sur la table Hive.
    _clauses = ", ".join(CLIENT_CLAUSES)      if CLIENT_CLAUSES      else "TOUTES"
    _types   = ", ".join(CLIENT_TYPE_CLAUSES) if CLIENT_TYPE_CLAUSES else "TOUS"
    logger.info(f"CPT chargé [vision={CLIENT_CPT_VISION}, clauses={_clauses}, types={_types}]")

    return df


# ============================================================================
# CHARGEMENT MRM
# ============================================================================

@timed_fn("load_mrm_raw")
def load_mrm_raw(
    spark   : SparkSession,
    cfg     : DatabaseConfig,
    path_key: str = "fichier_mrm",
) -> DataFrame:
    """
    Charge les données MRM depuis le fichier CSV.

    `path_key` choisit la source dans RUN_PARAMS :
        "fichier_mrm"    → MRM de l'inventaire courant (défaut)
        "fichier_mrm_n1" → MRM N+1, pour la détection des déclarations tardives

    Filtres appliqués dans l'ordre :
        1. Statut inventaire (Statut_INV) si CLIENT_MRM_STATUT_INV est défini
           (sinon aucun filtre statut, aucun count, aucun log).
        2. Type de compte (CLIENT_TYPE_CLAUSES) :
               traduit "PB" → valeur CSV MRM (ex: "CPB") via TYPE_CLAUSE_MRM_VALUE.
               Garantit que MRM et CPT couvrent le même périmètre de types.
        3. Clause (CLIENT_CLAUSES) :
               filtre sur `n_clause_ratta1` avec les numéros bruts (sans préfixe).

    Exemples :
        CLIENT_CLAUSES = ["121981"], CLIENT_TYPE_CLAUSES = ["PB"]
            → type_clause IN ["CPB"]  ET  n_clause_ratta1 IN ["121981"]
        CLIENT_CLAUSES = None, CLIENT_TYPE_CLAUSES = ["PB"]
            → type_clause IN ["CPB"]  (toutes les clauses PB)
        CLIENT_CLAUSES = None, CLIENT_TYPE_CLAUSES = None
            → pas de filtre (périmètre complet)

    Args:
        spark : SparkSession active
        cfg   : DatabaseConfig (contient le séparateur CSV)

    Returns:
        DataFrame MRM brut filtré (périmètre cohérent avec CPT)
    """
    mrm_path = RUN_PARAMS.get(path_key)
    if not mrm_path:
        raise ValueError(f"RUN_PARAMS['{path_key}'] est absent ou vide.")

    # Fix T-02 : inferSchema=false → toutes colonnes en string, casts ciblés
    # dans clean_mrm (to_date pour les dates, regexp_replace+cast double pour PM/PSAP).
    # Évite : (1) double-pass du CSV, (2) dates inférées comme string aléatoirement.
    df = (
        spark.read
             .option("header",      True)
             .option("delimiter",   cfg.mrm_delimiter)
             .option("inferSchema", "false")
             .option("encoding",    "UTF-8")
             .option("nullValue",   "")
             .csv(mrm_path)
    )

    # ── Filtre 0 : statut inventaire (optionnel) ──────────────────────────────
    if CLIENT_MRM_STATUT_INV:
        n_total  = df.count()
        df       = df.filter(F.col("Statut_INV") == CLIENT_MRM_STATUT_INV)
        n_actifs = df.count()
        logger.info(
            "MRM Statut_INV=%s : %d lignes totales → %d retenues / %d écartées",
            CLIENT_MRM_STATUT_INV, n_total, n_actifs, n_total - n_actifs,
        )

    # ── Filtre 1 : type de compte ─────────────────────────────────────────────
    if CLIENT_TYPE_CLAUSES:
        mrm_type_values = [
            TYPE_CLAUSE_MRM_VALUE[t]
            for t in CLIENT_TYPE_CLAUSES
            if t in TYPE_CLAUSE_MRM_VALUE
        ]
        if mrm_type_values:
            if MRM_TYPE_CLAUSE_COL in df.columns:
                df = df.filter(F.col(MRM_TYPE_CLAUSE_COL).isin(mrm_type_values))
            elif DEV_MODE:
                logger.warning(
                    f"Colonne '{MRM_TYPE_CLAUSE_COL}' absente du CSV MRM — "
                    f"filtre type_clause non appliqué. "
                    f"Vérifier MRM_TYPE_CLAUSE_COL dans settings.py (valeur actuelle: '{MRM_TYPE_CLAUSE_COL}')."
                )

    # ── Filtre 2 : clause ─────────────────────────────────────────────────────
    if CLIENT_CLAUSES:
        # MRM stocke le numéro seul dans n_clause_ratta1 — filtre direct
        df = df.filter(F.col("n_clause_ratta1").isin(CLIENT_CLAUSES))

    _clauses = ", ".join(CLIENT_CLAUSES)      if CLIENT_CLAUSES      else "TOUTES"
    _types   = ", ".join(CLIENT_TYPE_CLAUSES) if CLIENT_TYPE_CLAUSES else "TOUS"
    logger.info(f"MRM chargé [{path_key}, clauses={_clauses}, types={_types}]")

    return df
