"""Helpers internes et constantes partagées des analyses CPT/MRM."""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import Dict, List, Optional, Tuple

from config import MATCH_LABELS, DATE_INVENTAIRE, TYPE_CLAUSE_CPT_PREFIX
from modules.matching import categorize_mrm_conclusion

# Préfixe CPT → type de clause (ex. "CPB" → "PB"). Réciproque de
# TYPE_CLAUSE_CPT_PREFIX, pour dériver le type des dossiers sans contrepartie MRM.
_CPT_PREFIX_TO_TYPE = {v.rstrip("_"): t for t, v in TYPE_CLAUSE_CPT_PREFIX.items()}


def derive_clause_column(df: DataFrame) -> DataFrame:
    """
    Ajoute les colonnes CLAUSE et TYPE_CLAUSE attendues par les analyses
    multi-clause (analyze_suivi_consignes, calculate_taux_chute, …).

    Après le waterfall, la clause est portée par CPT_CLAUSE (ex. "CPB_121981",
    préfixe = type) et/ou MRM_CLAUSE (ex. "121981") ; il n'existe pas de colonne
    "CLAUSE" nue → d'où l'erreur UNRESOLVED_COLUMN quand on appelle run_full_analysis
    directement. On la dérive ici, sans aucune hypothèse mono-client :

        CLAUSE      = MRM_CLAUSE sinon CPT_CLAUSE sans son préfixe ("CPB_…").
        TYPE_CLAUSE = MRM_TYPE_CLAUSE sinon type déduit du préfixe CPT
                      (CPT_ONLY : pas de MRM → on lit le type dans "CPB_…").

    À appeler une fois sur df_result avant les analyses multi-clause.
    """
    clause_parts = []
    if "MRM_CLAUSE" in df.columns:
        clause_parts.append(F.col("MRM_CLAUSE"))
    if "CPT_CLAUSE" in df.columns:
        clause_parts.append(F.regexp_replace(F.col("CPT_CLAUSE"), r"^[A-Za-z]+_", ""))
    clause = F.coalesce(*clause_parts) if clause_parts else F.lit(None).cast("string")

    type_parts = []
    if "MRM_TYPE_CLAUSE" in df.columns:
        type_parts.append(F.col("MRM_TYPE_CLAUSE"))
    if "CPT_CLAUSE" in df.columns:
        # Préfixe CPT (ex. "CPB") → type ("PB") pour les dossiers sans MRM.
        prefix = F.regexp_extract(F.col("CPT_CLAUSE"), r"^([A-Za-z]+)_", 1)
        type_from_cpt = F.lit(None).cast("string")
        for pfx, t in _CPT_PREFIX_TO_TYPE.items():
            type_from_cpt = F.when(prefix == pfx, F.lit(t)).otherwise(type_from_cpt)
        type_parts.append(type_from_cpt)
    type_clause = F.coalesce(*type_parts) if type_parts else F.lit(None).cast("string")

    return df.withColumn("CLAUSE", clause).withColumn("TYPE_CLAUSE", type_clause)


def _with_mrm_action(df: DataFrame, conclusion_col: str) -> DataFrame:
    """Ajoute la colonne MRM_ACTION via categorize_mrm_conclusion."""
    return df.withColumn(
        "MRM_ACTION",
        categorize_mrm_conclusion(F.col(conclusion_col))
    )


def _statut_inv_dim(df: DataFrame, col: str = "MRM_STATUT_INV") -> List[str]:
    """
    Renvoie [col] si la colonne statut inventaire est présente, sinon [].

    Permet d'ajouter MRM_STATUT_INV comme dimension de ventilation (OUI/NON) dans
    les agrégats exportés sans casser quand la source ne fournit pas la colonne.
    Un MRM avec statut NON n'est pas remonté à la direction financière : la
    dissociation OUI/NON est portée par cette dimension côté Power BI.
    """
    return [col] if col in df.columns else []


def _filter_matched_keep_add_study(df: DataFrame) -> DataFrame:
    """Filtre les dossiers de l'univers chute (KEEP / STUDY / ADD).

    Univers UNIQUE = matchés inventaire courant + récupérés N+1 (CPT_LATE), via
    _matched_universe(). Garantit la cohérence du taux de chute par consigne ↔
    global ↔ niveaux de PM (cf. docs/METRIQUES.md §4). DELETE exclu (la chute n'y
    a pas de sens). CPT_OBS_TARDIVE exclu (jamais matché)."""
    return df.filter(
        F.col("TYPE_RECONCILIATION").isin(_matched_universe()) &
        F.col("MRM_ACTION").isin("MRM_KEEP", "MRM_ADD", "MRM_STUDY")
    )


# Tranches d'écart PM par défaut (en €)
# Utilisées pour la distribution sous/sur-provisionnement → bar chart viz
DEFAULT_ECART_TRANCHES: List[Tuple[Optional[float], Optional[float], str]] = [
    (None,    1_000,   "< 1K"),
    (1_000,   5_000,   "1K – 5K"),
    (5_000,   10_000,  "5K – 10K"),
    (10_000,  50_000,  "10K – 50K"),
    (50_000,  100_000, "50K – 100K"),
    (100_000, None,    "> 100K"),
]


# Tranches PM pour l'analyse des orphelins (en €)
DEFAULT_PM_TRANCHES: List[Tuple[Optional[float], Optional[float], str, int]] = [
    (None,    5_000,   "< 5K",       0),
    (5_000,   20_000,  "5K – 20K",   1),
    (20_000,  50_000,  "20K – 50K",  2),
    (50_000,  100_000, "50K – 100K", 3),
    (100_000, None,    "> 100K",     4),
]


def _pm_tranche_expr(
    pm_col  : str,
    tranches: Optional[List[Tuple[Optional[float], Optional[float], str, int]]] = None,
) -> Tuple[F.Column, F.Column]:
    """
    Retourne (TRANCHE_PM, ORDRE_TRANCHE) depuis une colonne PM.

    Tranches par défaut (€) :
        < 5K | 5K – 20K | 20K – 50K | 50K – 100K | > 100K

    Args:
        pm_col   : nom de la colonne PM source
        tranches : liste de (min, max, label, ordre) — défaut : DEFAULT_PM_TRANCHES

    Returns:
        Tuple (tranche_expr, ordre_expr) — deux colonnes Spark
    """
    active       = tranches or DEFAULT_PM_TRANCHES
    pm           = F.col(pm_col)
    tranche_expr = F.lit("AUTRE")
    ordre_expr   = F.lit(99)

    for (low, high, label, ordre) in reversed(active):
        if low is None and high is not None:
            cond = pm < high
        elif low is not None and high is None:
            cond = pm >= low
        else:
            cond = (pm >= low) & (pm < high)
        tranche_expr = F.when(cond, F.lit(label)).otherwise(tranche_expr)
        ordre_expr   = F.when(cond, F.lit(ordre)).otherwise(ordre_expr)

    return tranche_expr, ordre_expr


def _mois_label_expr(date_col: str) -> F.Column:
    """
    Abréviation française du mois (Jan … Déc) depuis une colonne date.

    Args:
        date_col : nom de la colonne date source (DateType)
    """
    m = F.month(F.col(date_col))
    return (
        F.when(m == 1,  "Jan")
         .when(m == 2,  "Fév")
         .when(m == 3,  "Mar")
         .when(m == 4,  "Avr")
         .when(m == 5,  "Mai")
         .when(m == 6,  "Jun")
         .when(m == 7,  "Jul")
         .when(m == 8,  "Aoû")
         .when(m == 9,  "Sep")
         .when(m == 10, "Oct")
         .when(m == 11, "Nov")
         .otherwise("Déc")
    )


def _inventory_year() -> Optional[int]:
    """Année d'inventaire dérivée de DATE_INVENTAIRE ('dd/MM/yyyy'). None si 'auto'."""
    try:
        return int(str(DATE_INVENTAIRE).split("/")[-1])
    except (ValueError, AttributeError):
        return None


# Colonnes d'identité MRM (alignées sur mrm_dup_keys) pour compter les dossiers
# MRM distincts et repérer le fan-out (1 MRM matché par plusieurs CPT).
_MRM_ID_COLS = ["MRM_IDCORP", "MRM_D_NAISSANCE", "MRM_D_SURVENANCE", "MRM_GARANTIE"]


def _matched_universe() -> List[str]:
    """Matchs légitimes + récupérés N+1 réels (= dossiers ayant une contrepartie
    MRM qui a matché). N'inclut PAS CPT_OBS_TARDIVE (anomalie, jamais matchée)."""
    return list(MATCH_LABELS) + ["CPT_LATE"]


def _mrm_identity(df: DataFrame) -> F.Column:
    """Identifiant d'un dossier MRM = concat des colonnes d'identité présentes."""
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in _MRM_ID_COLS if c in df.columns]
    return F.concat_ws("|", *cols) if cols else F.lit("∅")


# Ordre d'affichage et libellés des consignes.
_CONSIGNE_ORDRE = {"MRM_KEEP": 0, "MRM_STUDY": 1, "MRM_ADD": 2, "MRM_DELETE": 3}

