"""Diagnostics de réconciliation (fan-out CPT/MRM)."""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import Dict, List, Optional, Tuple

from config import MATCH_LABELS
from modules.analysis.helpers import _mrm_identity


def diagnose_mrm_fanout(
    df_result   : DataFrame,
    df_mrm_clean : Optional[DataFrame] = None,
    top         : int = 30,
) -> DataFrame:
    """
    Explique l'écart entre le MRM nettoyé (ex: 5121) et le mrm_nb de la synthèse
    (ex: 5209). Hypothèse : fan-out CPT — une même ligne MRM matchée par plusieurs
    lignes CPT est comptée plusieurs fois côté "matchés" (nb_match).

    Affiche un récapitulatif chiffré :
        - lignes matchées (nb_match, côté CPT après join)
        - dossiers MRM distincts matchés
        - sur-comptage = lignes matchées − MRM distincts (= fan-out CPT)
        - [si df_mrm_clean fourni] réconciliation au total MRM en entrée

    Renvoie le DÉTAIL des dossiers MRM matchés par > 1 CPT (à inspecter).

    Aucune correction de mrm_nb n'est faite ici : on confirme d'abord la cause.
    """
    matched = df_result.filter(F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS)))
    matched = matched.withColumn("_MRM_ID", _mrm_identity(matched))

    nb_matched_rows = matched.count()
    nb_mrm_distinct = matched.select("_MRM_ID").distinct().count()
    fanout_overcount = nb_matched_rows - nb_mrm_distinct

    print("\n[DIAG fan-out] écart MRM clean ↔ synthèse mrm_nb")
    print(f"  lignes matchées (nb_match, côté CPT)   : {nb_matched_rows:,}")
    print(f"  dossiers MRM distincts matchés         : {nb_mrm_distinct:,}")
    print(f"  → sur-comptage fan-out CPT (≈ écart)   : {fanout_overcount:,}")

    if df_mrm_clean is not None:
        n_mrm_in = df_mrm_clean.count()
        n_mrm_id = df_mrm_clean.withColumn("_MRM_ID", _mrm_identity(df_mrm_clean)) \
                               .select("_MRM_ID").distinct().count()
        print(f"  MRM clean en entrée (lignes)           : {n_mrm_in:,}")
        print(f"  MRM clean en entrée (identités dist.)  : {n_mrm_id:,}")
        print(f"  → doublons d'identité MRM en entrée    : {n_mrm_in - n_mrm_id:,}")

    # Détail : dossiers MRM matchés par plusieurs CPT, PM décroissante.
    nom_col = "MRM_NOM_PRENOM" if "MRM_NOM_PRENOM" in matched.columns else None
    detail = (
        matched
        .groupBy("_MRM_ID")
        .agg(
            F.count("*").alias("NB_CPT"),
            F.round(F.first("MRM_PM"), 2).alias("MRM_PM"),
            F.collect_set("TYPE_RECONCILIATION").alias("ETAPES"),
            *( [F.first(nom_col).alias("NOM_PRENOM")] if nom_col else [] ),
        )
        .filter(F.col("NB_CPT") > 1)
        .orderBy(F.desc("NB_CPT"), F.desc("MRM_PM"))
    )
    print(f"  dossiers MRM en fan-out (NB_CPT > 1) — top {top} :")
    return detail

