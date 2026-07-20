"""
Orchestration de la couche métriques — les 9 tables, et leur export.

toutes_metriques : les 9 tables pandas en un dict (8 tables-sujets + la
dimension de run `dim_run`, contrôles inclus).
export_metriques : écriture multi-format (Excel privilégié, CSV/JSON/Parquet
DBFS, Delta si schéma) sous <EXPORT_BASE_PATH>/<CLIENT>_<PERIM>/metrics.
Toutes les tables portent la clé de liaison CLE_RUN (DATE_INVENTAIRE ×
PERIMETRE) : le modèle en étoile Power BI se construit sans transformation.
"""

import os
from typing import Dict, Iterable, Optional

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from config import CLIENT_NAME, EXPORT_BASE_PATH
from core.io.excel_export import export_excel
from core.io.save_result import cle_run, to_date_iso, write_delta_historise
from core.synthese.kpi_export import compute_synthese
from core.synthese.synthese_contract import SyntheseScalars
from core.metrics.base import _PERIMETRE, _to_local, output_dir, _annee_inventaire
from core.metrics.scalaires import dim_run, synthese, bilan_cas, consignes, couverture
from core.metrics.agregats import chute, consignes_par_type_compte, orphelins
from core.metrics.coherence import controles_coherence


# ============================================================================
# ORCHESTRATION
# ============================================================================

def toutes_metriques(df_result: DataFrame, d: Optional[SyntheseScalars] = None) -> Dict[str, pd.DataFrame]:
    """Les 9 tables métriques en un dict {nom: DataFrame pandas}.

    Une table = un sujet complet (contrat : docs/METRIQUES.md §6) ; les angles
    d'analyse sont des COLONNES (EXERCICE, AXE, SEGMENT, UNIVERS), jamais des
    tables séparées :

        dim_run                   — la dimension de run (pivot du modèle en étoile)
        synthese                  — tous les KPI, une ligne par run
        bilan_cas                 — LE bilan cas par cas (avec explications)
        couverture                — les deux univers (Compte / Revue MRM)
        chute                     — le taux de chute sous tous ses angles
        consignes                 — le suivi des consignes, les deux exercices
        consignes_par_type_compte — tableau de bord TYPE_COMPTE × CONSIGNE
        orphelins                 — l'investigation des orphelins, six angles
        controles_coherence       — recoupements inter-tables (attendu/obtenu/OK)

    `d` = dict de compute_synthese si déjà calculé (ex. retour de
    print_synthese) — sinon la passe Spark est lancée ici.
    """
    d = d if d is not None else compute_synthese(df_result)
    tables = {
        "dim_run"                  : dim_run(d),
        "synthese"                 : synthese(d),
        "bilan_cas"                : bilan_cas(d),
        "couverture"               : couverture(d),
        "chute"                    : chute(df_result, d),
        "consignes"                : consignes(d),
        "consignes_par_type_compte": consignes_par_type_compte(df_result),
        "orphelins"                : orphelins(df_result, _annee_inventaire(d)),
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
    run DATE_INVENTAIRE, PERIMETRE, LIBELLE_RUN et la clé de liaison CLE_RUN
    (« <date ISO>|<périmètre> ») — la colonne de relation du modèle en étoile
    Power BI, dont la table `dim_run` est le pivot (1-n vers chaque table).
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
        pdf["CLE_RUN"]         = cle_run(date_iso or d["date_inventaire"], _PERIMETRE)

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
