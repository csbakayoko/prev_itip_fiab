"""
Orchestration de la couche métriques — toutes les tables, et leur export.

toutes_metriques : toutes les tables pandas en un dict (contrôles inclus).
export_metriques : écriture multi-format (Excel privilégié, CSV/JSON/Parquet
DBFS, Delta si schéma) sous <EXPORT_BASE_PATH>/<CLIENT>_<PERIM>/metrics.
"""

import os
from typing import Dict, Iterable, Optional

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from config import CLIENT_NAME, EXPORT_BASE_PATH
from core.io.excel_export import export_excel
from core.io.save_result import to_date_iso, write_delta_historise
from core.synthese.kpi_export import compute_synthese
from core.synthese.synthese_contract import SyntheseScalars
from core.metrics.base import _PERIMETRE, _to_local, output_dir, _annee_inventaire
from core.metrics.scalaires import (
    synthese, bilan_cas, taux_chute, chute_par_exercice, suivi_n1, consignes,
    compte_justification, couverture_mrm, conformite_globale,
    chute_par_consigne, pm_par_consigne, conformite_consignes,
)
from core.metrics.agregats import (
    chute_par_clause, chute_par_anciennete, anomalies_cpt_only,
    consignes_par_clause,
    orphelins_par_clause, orphelins_par_garantie, orphelins_par_anciennete,
    orphelins_cles_nulles,
)
from core.metrics.coherence import controles_coherence


# ============================================================================
# ORCHESTRATION
# ============================================================================

def toutes_metriques(df_result: DataFrame, d: Optional[SyntheseScalars] = None) -> Dict[str, pd.DataFrame]:
    """Toutes les métriques en un dict {nom: DataFrame pandas}.

    `d` = dict de compute_synthese si déjà calculé (ex. retour de
    print_synthese) — sinon la passe Spark est lancée ici.
    """
    d = d if d is not None else compute_synthese(df_result)
    annee = _annee_inventaire(d)
    tables = {
        "synthese"             : synthese(d),
        "bilan_cas"            : bilan_cas(d),
        "taux_chute"           : taux_chute(d),
        "chute_par_exercice"   : chute_par_exercice(d),
        "suivi_n1"             : suivi_n1(d),
        "consignes"            : consignes(d),
        "consignes_par_clause" : consignes_par_clause(df_result),
        "compte_justification" : compte_justification(d),
        "couverture_mrm"       : couverture_mrm(d),
        "chute_par_clause"     : chute_par_clause(df_result),
        "chute_par_anciennete" : chute_par_anciennete(df_result, annee),
        "chute_par_consigne"   : chute_par_consigne(d),
        "conformite_consignes" : conformite_consignes(d),
        "anomalies_cpt_only"   : anomalies_cpt_only(df_result),
        # Investigation des orphelins CPT_ONLY (compte préposé).
        "orphelins_par_clause"    : orphelins_par_clause(df_result),
        "orphelins_par_garantie"  : orphelins_par_garantie(df_result),
        "orphelins_par_anciennete": orphelins_par_anciennete(df_result, annee),
        "orphelins_cles_nulles"   : orphelins_cles_nulles(df_result),
        "conformite_globale"   : conformite_globale(d),
        "pm_par_consigne"      : pm_par_consigne(d),
    }
    # Les onglets Power BI doivent se recouper : contrôles inter-tables,
    # exportés avec le reste (bloquants dans le run de production).
    tables["controles_coherence"] = controles_coherence(tables, d)
    return tables


def export_metriques(
    df_result   : DataFrame,
    d           : Optional[SyntheseScalars] = None,
    base_path   : str = EXPORT_BASE_PATH,
    formats     : Iterable[str] = ("excel", "csv", "parquet"),
    delta_schema: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Écrit toutes les métriques sur DBFS, sous <base>/<CLIENT>_<PERIM>/metrics.

    formats ⊆ {excel, csv, parquet, json, delta} — EXCEL est le format
    privilégié (classeur multi-onglets propre, onglets nommés par axe d'étude,
    tables Excel détectées par Power BI — cf. core/io/excel_export.py) ; delta
    requiert delta_schema (une table <schema>.metrique_<nom> par métrique —
    nom STABLE, le périmètre est une colonne), créée si absente et HISTORISÉE
    par run (replaceWhere sur DATE_INVENTAIRE × PERIMETRE : rejouer un run
    remplace SES lignes, les autres inventaires/périmètres coexistent).

    Schéma standard des exports : toutes les tables portent les colonnes de
    run DATE_INVENTAIRE, PERIMETRE et LIBELLE_RUN.
    """
    d       = d if d is not None else compute_synthese(df_result)
    tables  = toutes_metriques(df_result, d)
    formats = {f.lower() for f in formats}

    # Colonnes de run (schéma standard) : date ISO si résoluble — l'export
    # Delta, lui, EXIGE une date résoluble (pas d'historisation aveugle).
    date_iso = to_date_iso(d["date_inventaire"], strict="delta" in formats and bool(delta_schema))
    for pdf in tables.values():
        pdf["DATE_INVENTAIRE"] = date_iso or d["date_inventaire"]
        pdf["PERIMETRE"]       = _PERIMETRE
        pdf["LIBELLE_RUN"]     = CLIENT_NAME

    out = _to_local(output_dir(base_path, "metrics"))
    os.makedirs(out, exist_ok=True)
    print(f"[METRICS] périmètre {CLIENT_NAME} / clauses {_PERIMETRE} → {sorted(formats)}")

    ctrl = tables["controles_coherence"]
    ko = ctrl[~ctrl["OK"]]
    if len(ko):
        print(f"[METRICS] ✘ {len(ko)} contrôle(s) inter-tables KO — les onglets Power BI ne se recoupent pas :")
        for _, r in ko.iterrows():
            print(f"    ✘ {r['CONTROLE']} : attendu {r['ATTENDU']}, obtenu {r['OBTENU']}")
    else:
        print(f"[METRICS] ✔ contrôles inter-tables : {len(ctrl)}/{len(ctrl)} OK")

    for name, pdf in tables.items():
        if "csv" in formats:
            pdf.to_csv(f"{out}/{name}.csv", index=False, sep=";", encoding="utf-8")
            print(f"  ✓ [CSV]     {out}/{name}.csv")
        if "json" in formats:
            pdf.to_json(f"{out}/{name}.json", orient="records",
                        force_ascii=False, indent=2, date_format="iso")
            print(f"  ✓ [JSON]    {out}/{name}.json")
        if "parquet" in formats:
            pdf.to_parquet(f"{out}/{name}.parquet", index=False)
            print(f"  ✓ [PARQUET] {out}/{name}.parquet")
        if "delta" in formats and delta_schema:
            table = f"{delta_schema}.metrique_{name}"
            sdf = (df_result.sparkSession.createDataFrame(pdf)
                            .withColumn("DATE_INVENTAIRE", F.lit(date_iso).cast("date")))
            write_delta_historise(sdf, table, date_iso)
            print(f"  ✓ [DELTA]   {table}  (run {date_iso} / {_PERIMETRE} remplacé)")

    if "excel" in formats:
        path = f"{out}/metrics_{CLIENT_NAME}_{_PERIMETRE}.xlsx"
        export_excel(tables, path, client=CLIENT_NAME, perimetre=_PERIMETRE)
        print(f"  ✓ [EXCEL]   {path}  (onglets par axe + Sommaire, prêt Power BI)")

    print(f"[METRICS] export terminé → {out}\n")
    return tables
