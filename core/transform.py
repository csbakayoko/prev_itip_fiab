"""
Transformations Spark génériques et réutilisables.
Nettoyage, standardisation, dédoublonnage, clés de matching.
"""

import logging

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from typing import Dict, List, Optional, Tuple

from config import MAPPING_CPT, MAPPING_MRM, TechnicalConfig, tech_cfg
from core._timing import timed_fn

logger = logging.getLogger(__name__)


# ============================================================================
# SÉLECTION / RENOMMAGE
# ============================================================================

def select_and_rename(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """
    Sélectionne et renomme les colonnes selon le mapping.
    Logue un avertissement pour chaque colonne source absente du DataFrame
    (changement de nom en source, colonne supprimée…).
    """
    missing = [src for src in mapping if src not in df.columns]
    if missing:
        logger.warning("Colonnes absentes du DataFrame (vérifier la source) : %s", missing)
    exprs = [F.col(src).alias(dst) for src, dst in mapping.items() if src in df.columns]
    if not exprs:
        raise ValueError("Aucune colonne du mapping trouvée dans le DataFrame.")
    return df.select(*exprs)


# ============================================================================
# NETTOYAGE TEXTE
# ============================================================================

def normalize_name_full(col: F.Column) -> F.Column:
    """
    Normalisation canonique d'un NOM_PRENOM pour les clés de matching.

    Étapes :
        1. trim
        2. upper (T-05 — élimine 'Jean Dupont' vs 'JEAN DUPONT')
        3. suppression des espaces

    Note : pas de strip d'accents — Spark/JVM gère les caractères Unicode de façon
    cohérente entre CPT (Hive) et MRM (CSV) si l'encodage source est uniforme.
    """
    return F.regexp_replace(F.upper(F.trim(col)), r"\s+", "")


def normalize_name_truncated(col: F.Column, n: int = 20) -> F.Column:
    """
    Normalise NOM_PRENOM en prenant les N premiers caractères puis en supprimant
    les espaces — alignement sur la limite technique CPT (20 caractères max).

    Problème identifié :
        Le système CPT tronque NOM_PRENOM à 20 caractères lors de la saisie.
        MRM stocke le nom complet. La troncature coupe parfois à l'intérieur
        du dernier token (prénom), rendant les autres clés inopérantes.

        Ex (n=20) :
            CPT : "REICHENAUER CHRISTEL"   → 20 chars → "REICHENAUERCHRISTEL"
            MRM : "REICHENAUER CHRISTELLE" → 20 chars → "REICHENAUERCHRISTEL"
            → Identiques ✓

            CPT : "MOURA PINTO ANA ADEL"   → 20 chars → "MOURAPINTOANAAADEL"
            MRM : "MOURA PINTO ANA ADELAIDE" → 20 chars → "MOURAPINTOANAAADEL"
            → Identiques ✓

    Args:
        col : Colonne Spark contenant NOM_PRENOM
        n   : Nombre de caractères à conserver (défaut : 20 = limite CPT)

    Returns:
        Colonne Spark : LEFT(NOM_PRENOM, n) uppercase, sans espaces
    """
    return F.regexp_replace(
        F.substring(F.upper(F.trim(col)), 1, n),
        r"\s+", "",
    )


# ============================================================================
# CLÉS DE MATCHING
# ============================================================================

def add_matching_keys(
    df            : DataFrame,
    rpp_col       : str,
    nom_prenom_col: str = "NOM_PRENOM",
    naissance_col : str = "D_NAISSANCE",
    survenance_col: str = "D_SURVENANCE",
    garantie_col  : str = "GARANTIE",
    clause_col    : str = "CLAUSE",
) -> DataFrame:
    """
    Ajoute les clés composites de rapprochement CPT/MRM.

    Stratégie en cascade — de la plus stricte à la plus flexible :

    ┌───────────────────────────┬───────────────────────────┬─────────────────────────┬──────────────────────────────────────────────────────┐
    │ Clé                       │ D_SURVENANCE              │ Identité                │ Usage                                                │
    ├───────────────────────────┼───────────────────────────┼─────────────────────────┼──────────────────────────────────────────────────────┤
    │ key_strict                │ jour (exact)              │ NOM_PRENOM complet      │ MATCH_EXACT — matching nominal                       │
    │ key_no_date               │ —                         │ NOM_PRENOM complet      │ MATCH_WINDOW / MATCH_RECHUTE (avec garantie)         │
    │ key_strict_tronc          │ jour (exact)              │ LEFT(NOM_PRENOM, 20)    │ MATCH_TRONC — troncature CPT 20 chars                │
    │ key_no_date_tronc         │ —                         │ LEFT(NOM_PRENOM, 20)    │ MATCH_TRONC_WINDOW (troncature + fenêtre)            │
    │ key_no_garantie           │ jour (exact)              │ NOM_PRENOM complet      │ MATCH_IP — passage IT→IP (offset garantie)           │
    └───────────────────────────┴───────────────────────────┴─────────────────────────┴──────────────────────────────────────────────────────┘

    "Tronqué" (key_*_tronc) : le CPT est limité à 20 caractères dans NOM_PRENOM.
    La troncature peut couper à l'intérieur du dernier token, rendant key_strict inopérante.
    → Les deux côtés (CPT et MRM) appliquent LEFT(20) uppercase + strip espaces avant de constituer la clé.
    Ex : "REICHENAUER CHRISTELLE" (MRM) → LEFT(20) = "REICHENAUER CHRISTEL" → même clé que CPT ✓

    CLÉS DE SECOURS « CLAUSE » (key_clause_*) : le RPP est remplacé par le numéro
    de clause normalisé. Elles rattrapent les dossiers dont le RPP compte est nul
    ou mal renseigné — toutes les clés ci-dessus échouent alors et le dossier
    finit en CPT_ONLY définitif malgré une vraie contrepartie. Placées en fin de
    waterfall (les clés RPP matchent d'abord), elles déclinent les mêmes variantes
    nom complet / nom tronqué × date stricte / fenêtre :
        key_clause_strict / _no_date / _strict_tronc / _no_date_tronc
    La clause est moins discriminante que le RPP (partagée par tout un contrat) :
    on ne décline PAS les étapes IP / rechute en clause (plus risquées sur une clé
    sans RPP, hors périmètre). Garde anti-collision : la clé vaut NULL quand la
    clause est absente, sinon concat_ws l'ignorerait et la clé matcherait sur
    dob+nom+date entre clauses différentes.

    Args:
        df             : DataFrame à enrichir
        rpp_col        : Colonne RPP
        nom_prenom_col : Colonne nom/prénom complet (ex: "DUPONT JEAN MARIE")
        naissance_col  : Colonne date de naissance
        survenance_col : Colonne date de survenance
        garantie_col   : Colonne code garantie
        clause_col     : Colonne numéro de clause (préfixe type côté CPT toléré)

    Returns:
        DataFrame enrichi avec les 5 clés RPP + 4 clés de secours clause
    """
    required = [rpp_col, nom_prenom_col, naissance_col, survenance_col, garantie_col]
    missing  = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes pour les clés de matching : {missing}")

    rpp        = F.col(rpp_col).cast("string")
    dob        = F.date_format(F.col(naissance_col),  "yyyyMMdd")
    dos_strict = F.date_format(F.col(survenance_col), "yyyyMMdd")
    garantie   = F.col(garantie_col).cast("int")
    nom_full   = normalize_name_full(F.col(nom_prenom_col))
    nom_tronc  = normalize_name_truncated(F.col(nom_prenom_col), n=20)

    _key = lambda *parts: F.concat_ws("", *parts)

    df = (
        df
        .withColumn("key_strict",        _key(rpp, dob, dos_strict, garantie, nom_full))
        .withColumn("key_no_date",       _key(rpp, dob,             garantie, nom_full))
        .withColumn("key_strict_tronc",  _key(rpp, dob, dos_strict, garantie, nom_tronc))
        .withColumn("key_no_date_tronc", _key(rpp, dob,             garantie, nom_tronc))
        # Clé sans garantie (survenance au jour) — étape passage IT → IP
        .withColumn("key_no_garantie",   _key(rpp, dob, dos_strict, nom_full))
    )

    # Clés de secours « clause » : RPP remplacé par la clause normalisée (préfixe
    # type CPT retiré, ex. "CPB_121981" → "121981" ; MRM "121981" inchangé). NULL
    # si la clause est absente (cf. garde anti-collision dans la docstring).
    if clause_col in df.columns:
        clause_norm = F.regexp_replace(
            F.upper(F.trim(F.col(clause_col).cast("string"))), r"^[A-Z]+_", ""
        )
        has_clause = clause_norm.isNotNull() & (clause_norm != "")
        _ckey = lambda *parts: F.when(has_clause, _key(clause_norm, *parts))
        df = (
            df
            .withColumn("key_clause_strict",        _ckey(dob, dos_strict, garantie, nom_full))
            .withColumn("key_clause_no_date",       _ckey(dob,             garantie, nom_full))
            .withColumn("key_clause_strict_tronc",  _ckey(dob, dos_strict, garantie, nom_tronc))
            .withColumn("key_clause_no_date_tronc", _ckey(dob,             garantie, nom_tronc))
        )

    return df


# ============================================================================
# DÉDOUBLONNAGE
# ============================================================================

def keep_latest_by_keys(
    df       : DataFrame,
    keys     : List[str],
    order_col: str,
) -> DataFrame:
    """
    Dédoublonnage technique : garde la version la plus récente par clé.

    Args:
        df        : DataFrame source
        keys      : colonnes de partitionnement
        order_col : colonne de tri descendant (ex: tech_day)
    """
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def dedupe_mrm_by_strict_key(
    df        : DataFrame,
    key_col   : str = "key_strict",
    statut_col: str = "STATUT_INV",
) -> DataFrame:
    """
    Dédoublonnage MRM sur la clé stricte — déterministe et justifié.

    Règle métier : quand plusieurs lignes partagent la même clé stricte, on
    retire EN PRIORITÉ celles au statut inventaire NON (non remontées à la
    direction financière), puis on garde la plus récente (date d'inventaire),
    avec un départage stable (PM décroissant, puis n° de sinistre) → résultat
    reproductible d'un run à l'autre (corrige le non-déterminisme qui avait fait
    désactiver le dropDuplicates historique).

    Le nettoyage est tracé : nombre de doublons retirés, dont combien au statut
    NON (justification du nettoyage avant les métriques).
    """
    if key_col not in df.columns:
        return df

    is_non = (
        (F.upper(F.trim(F.col(statut_col))) == F.lit("NON"))
        if statut_col in df.columns else F.lit(False)
    )

    # Priorité : OUI/autre (0) avant NON (1) ; puis plus récent ; puis stable.
    order = [F.when(is_non, 1).otherwise(0).asc()]
    if "D_INVENTAIRE" in df.columns:
        order.append(F.col("D_INVENTAIRE").desc_nulls_last())
    if "PM" in df.columns:
        order.append(F.col("PM").desc_nulls_last())
    if "NUM_SINISTRE" in df.columns:
        order.append(F.col("NUM_SINISTRE").asc_nulls_last())

    w      = Window.partitionBy(key_col).orderBy(*order)
    ranked = df.withColumn("_rn", F.row_number().over(w))

    # Justification du nettoyage (une seule passe d'agrégation sur les retirés).
    stats = (
        ranked.filter(F.col("_rn") > 1)
        .select(
            F.count(F.lit(1)).alias("n_removed"),
            F.sum(F.when(is_non, 1).otherwise(0)).alias("n_removed_non"),
        )
        .first()
    )
    n_removed     = (stats["n_removed"] or 0) if stats else 0
    n_removed_non = (stats["n_removed_non"] or 0) if stats else 0

    if n_removed:
        logger.info(
            "Dédoublonnage MRM clé stricte : %d doublon(s) retiré(s) "
            "(dont %d statut NON, %d autres départagés par date d'inventaire).",
            n_removed, n_removed_non, n_removed - n_removed_non,
        )
    else:
        logger.info("Dédoublonnage MRM clé stricte : aucun doublon détecté.")

    return ranked.filter(F.col("_rn") == 1).drop("_rn")


# ============================================================================
# CONVERSIONS
# ============================================================================

def cast_mrm_amounts(df: DataFrame, cols: List[str]) -> DataFrame:
    """Convertit les montants MRM (séparateur virgule → double)."""
    for c in cols:
        df = df.withColumn(c, F.regexp_replace(F.col(c), ",", ".").cast("double"))
    return df


# ============================================================================
# PRÉFIXAGE / COLONNES
# ============================================================================

def prefix_columns(
    df    : DataFrame,
    prefix: str,
    keep  : Optional[List[str]] = None,
) -> DataFrame:
    """
    Préfixe toutes les colonnes sauf celles dans keep.

    Args:
        df     : DataFrame source
        prefix : préfixe à ajouter (ex: "CPT_", "MRM_")
        keep   : colonnes à ne pas préfixer (ex: clés de matching)
    """
    keep = keep or []
    return df.select([
        F.col(c) if c in keep else F.col(c).alias(f"{prefix}{c}")
        for c in df.columns
    ])


def drop_duplicate_columns(df: DataFrame) -> DataFrame:
    """
    Supprime les colonnes dupliquées produites par un join Spark.
    Les doublons sont renommés __DUP_{idx} puis droppés.
    """
    seen     = set()
    new_cols = []
    for idx, c in enumerate(df.columns):
        if c not in seen:
            seen.add(c)
            new_cols.append(c)
        else:
            new_cols.append(f"{c}__DUP_{idx}")

    tmp     = df.toDF(*new_cols)
    to_drop = [c for c in tmp.columns if "__DUP_" in c]
    return tmp.drop(*to_drop)


# ============================================================================
# PIPELINES DE NETTOYAGE CPT / MRM (point d'entrée depuis api.py)
# ============================================================================

@timed_fn("clean_cpt")
def clean_cpt(df_raw: DataFrame, cfg: TechnicalConfig = tech_cfg) -> DataFrame:
    """
    Pipeline complet de nettoyage CPT :
        1. Sélection / renommage selon MAPPING_CPT
        2. Dédoublonnage technique (garde la ligne la plus récente)
        3. Ajout des clés de matching

    Args:
        df_raw : DataFrame CPT brut (sorti de loader.py)
        cfg    : TechnicalConfig (cpt_order_col, cpt_dup_keys)

    Returns:
        DataFrame CPT nettoyé, préfixé CPT_*, prêt pour le matching
    """
    df = keep_latest_by_keys(df_raw, list(cfg.cpt_dup_keys), cfg.cpt_order_col)
    df = select_and_rename(df, MAPPING_CPT)
    # CPT : dates Hive en date/timestamp → cast("date") (tolère les timestamps,
    # ex. "1978-09-16 00:00:00", et renvoie null sans lever d'exception).
    for date_col in ("D_NAISSANCE", "D_SURVENANCE", "D_INVALIDITE"):
        if date_col in df.columns:
            df = df.withColumn(date_col, F.col(date_col).cast("date"))
    df = add_matching_keys(df, rpp_col="RPP")
    df = prefix_columns(
        df, prefix="CPT_",
        keep=["key_strict", "key_no_date", "key_strict_tronc", "key_no_date_tronc",
              "key_no_garantie", "key_clause_strict", "key_clause_no_date",
              "key_clause_strict_tronc", "key_clause_no_date_tronc"],
    )
    return df


@timed_fn("clean_mrm")
def clean_mrm(df_raw: DataFrame, cfg: TechnicalConfig = tech_cfg) -> DataFrame:
    """
    Pipeline complet de nettoyage MRM :
        1. Sélection / renommage selon MAPPING_MRM
        2. Cast des dates (D_NAISSANCE, D_SURVENANCE) et montants (PM, PSAP)
        3. Ajout des clés de matching
        4. Dédoublonnage déterministe sur la clé stricte (priorité OUI > NON,
           puis plus récent) — tracé et justifié avant les métriques

    Args:
        df_raw : DataFrame MRM brut (sorti de loader.py)
        cfg    : TechnicalConfig (mrm_dup_keys)

    Returns:
        DataFrame MRM nettoyé, préfixé MRM_*, prêt pour le matching
    """
    df = select_and_rename(df_raw, MAPPING_MRM)
    # MRM : dates du CSV au format français → to_date(col, "dd/MM/yyyy").
    for date_col in ("D_NAISSANCE", "D_SURVENANCE", "D_INVENTAIRE", "D_INVALIDITE"):
        if date_col in df.columns:
            df = df.withColumn(date_col, F.to_date(F.col(date_col), "dd/MM/yyyy"))
    df = cast_mrm_amounts(df, cols=["PM", "PSAP"])
    df = add_matching_keys(df, rpp_col="IDCORP")
    # Dédoublonnage MRM sur la clé stricte (déterministe) : retire les doublons,
    # priorité OUI > NON puis plus récent. Évite le double-comptage des doublons
    # MRM (notamment en MRM_MISSING) et le fan-out de jointure.
    df = dedupe_mrm_by_strict_key(df)
    df = prefix_columns(
        df, prefix="MRM_",
        keep=["key_strict", "key_no_date", "key_strict_tronc", "key_no_date_tronc",
              "key_no_garantie", "key_clause_strict", "key_clause_no_date",
              "key_clause_strict_tronc", "key_clause_no_date_tronc"],
    )
    return df
