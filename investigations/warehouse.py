"""
Chargement et préparation des fichiers entrepôt (gros Excel multi-inventaire).

L'entrepôt contient l'historique : un même dossier apparaît à plusieurs dates
d'inventaire. On le prépare AVEC le même schéma de clés que le pipeline principal
(MAPPING_CPT / MAPPING_MRM + add_matching_keys) MAIS SANS dédoublonnage : toutes
les apparitions sont conservées pour pouvoir tracer l'historique.

Point d'attention — cohérence des clés avec les orphelins :
    CPT : composant RPP de la clé = colonne `RPP`   (n_rpp, cf. clean_cpt)
    MRM : composant RPP de la clé = colonne `IDCORP` (cf. clean_mrm)
Toute divergence ici ferait rater les jointures orphelin ↔ entrepôt.

Lecture via spark-excel (com.crealytics.spark.excel) en inferSchema=false :
toutes les colonnes arrivent en string, on applique ensuite les casts ciblés
(to_date sur les dates, virgule→point + double sur les montants), comme pour
le CSV MRM source.
"""

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from config import (
    MAPPING_CPT,
    MAPPING_MRM,
    EXCEL_FORMAT,
    EXCEL_SHEET,
    EXCEL_DATE_FORMAT,
)
from modules.transform import select_and_rename, add_matching_keys, prefix_columns, cast_mrm_amounts

# Clés conservées non préfixées (alignées sur clean_cpt / clean_mrm).
_KEEP_KEYS = ["key_strict", "key_no_date", "key_strict_tronc", "key_no_date_tronc", "key_no_garantie"]

# Colonne RPP de la clé selon la base (cf. docstring).
_RPP_COL = {"cpt": "RPP", "mrm": "IDCORP"}

# Colonnes date / montant canoniques à caster si présentes.
_DATE_COLS   = ("D_NAISSANCE", "D_SURVENANCE", "D_INVENTAIRE", "D_INVALIDITE")
_AMOUNT_COLS = ("PM", "PSAP", "PM_EXO_INV")


def load_warehouse_excel(
    spark: SparkSession,
    path : str,
    sheet: str = EXCEL_SHEET,
) -> DataFrame:
    """Lit un fichier entrepôt Excel via spark-excel (toutes colonnes en string)."""
    reader = (
        spark.read.format(EXCEL_FORMAT)
             .option("header",      "true")
             .option("inferSchema", "false")
    )
    if sheet:
        reader = reader.option("dataAddress", f"'{sheet}'!A1")
    return reader.load(path)


def prepare_warehouse(df_raw: DataFrame, base: str) -> DataFrame:
    """
    Met l'entrepôt brut au même format que les bases nettoyées, SANS dédoublonner.

    Args:
        df_raw : sortie de load_warehouse_excel.
        base   : "cpt" ou "mrm".

    Returns:
        DataFrame préfixé (CPT_* / MRM_*) avec les clés de matching non préfixées,
        une ligne par apparition (date d'inventaire) conservée.
    """
    if base not in _RPP_COL:
        raise ValueError(f"base inconnue : '{base}'. Attendu : 'cpt' ou 'mrm'.")

    mapping = MAPPING_CPT if base == "cpt" else MAPPING_MRM
    df = select_and_rename(df_raw, mapping)

    # Dates : l'Excel arrive en string → to_date au format EXCEL_DATE_FORMAT.
    for c in _DATE_COLS:
        if c in df.columns:
            df = df.withColumn(c, F.to_date(F.col(c), EXCEL_DATE_FORMAT))

    # Montants : format européen "12,34" toléré (regexp virgule→point + cast double).
    df = cast_mrm_amounts(df, [c for c in _AMOUNT_COLS if c in df.columns])

    df = add_matching_keys(df, rpp_col=_RPP_COL[base])
    df = prefix_columns(df, prefix=f"{base.upper()}_", keep=_KEEP_KEYS)
    return df
