# Databricks notebook source
# MAGIC %md
# MAGIC # 🔬 ITIP-FIAB — Comparaison des inventaires 2023 vs 2024 (sans écriture)
# MAGIC
# MAGIC Rejoue le pipeline complet **pour les deux années dans une même session**,
# MAGIC puis compare côte à côte :
# MAGIC - les **KPI de tête** (taux de chute, couverture, conformité, récupération) ;
# MAGIC - le **taux de chute par ancienneté** (N / N-1 / N-2+) — la méthode
# MAGIC   d'inventaire diffère selon l'année de survenance ;
# MAGIC - l'**investigation des orphelins** (par type de compte, par compte,
# MAGIC   garantie, nullité de clé).
# MAGIC
# MAGIC Mécanique : `core.runtime.configurer_run` surcharge, pour chaque année, la
# MAGIC date d'inventaire, la vision CPT et les fichiers MRM (valeurs liées par
# MAGIC valeur dans les modules) ; `main.build_df_result` construit `df_result`.
# MAGIC
# MAGIC **Aucune écriture** : ce notebook passe par `build_df_result` et calcule les
# MAGIC tables en mémoire. Il n'appelle ni `main.run` ni `export_metriques` — rien
# MAGIC ne part dans le metastore.
# MAGIC
# MAGIC ⚠ Avant de lancer : **renseigner les chemins MRM 2024** (widgets) — la vision
# MAGIC 2024 est CC2024.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup + widgets (sources par année)

# COMMAND ----------

import pandas as pd

from config import INVENTAIRES
from core.runtime import configurer_run, get_spark
from core.synthese.kpi_export import compute_synthese
from core import metrics
from core.metrics.viz import graph_chute_par_anciennete, graph_orphelins_par_compte
from main import build_df_result

spark = get_spark()

# COMMAND ----------

# Défauts par année : config/profile.py (INVENTAIRES) — source unique des
# chemins. Les widgets restent éditables au run (ex. MRM 2024 à ajuster).
for _annee, _inv in INVENTAIRES.items():
    dbutils.widgets.text(f"date_{_annee}",   _inv["date"],   f"{_annee} · date d'inventaire")
    dbutils.widgets.text(f"vision_{_annee}", _inv["vision"], f"{_annee} · vision CPT")
    dbutils.widgets.text(f"mrm_{_annee}",    _inv["mrm"],    f"{_annee} · MRM courant")
    dbutils.widgets.text(f"mrm_{_annee}_n1", _inv["mrm_n1"], f"{_annee} · MRM N+1 (option)")

ANNEES = tuple(INVENTAIRES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run des deux inventaires
# MAGIC
# MAGIC Pour chaque année : `configurer_run` → `build_df_result` → `compute_synthese`
# MAGIC → `toutes_metriques`. Les tables métriques sont des pandas (matérialisées),
# MAGIC donc `df_result` est libéré après chaque année (mémoire).

# COMMAND ----------

def run_annee(annee: str) -> dict:
    """Construit df_result pour l'année et renvoie {d, tables} (df libéré)."""
    profil = configurer_run(
        date_inventaire=dbutils.widgets.get(f"date_{annee}"),
        cpt_vision     =dbutils.widgets.get(f"vision_{annee}"),
        fichier_mrm    =dbutils.widgets.get(f"mrm_{annee}"),
        fichier_mrm_n1 =dbutils.widgets.get(f"mrm_{annee}_n1") or None,
    )
    print(f"\n===== Inventaire {annee} : {profil} =====")
    df = build_df_result(spark).persist()
    print(f"  df_result {annee} : {df.count():,} lignes")
    d = compute_synthese(df)
    tables = metrics.toutes_metriques(df, d)
    ctrl = tables["controles_coherence"]
    ko = int((~ctrl["OK"]).sum())
    print(f"  recoupements inter-tables : {len(ctrl) - ko}/{len(ctrl)} OK"
          + (f" — ✘ {ko} KO" if ko else ""))
    df.unpersist()
    return {"d": d, "tables": tables}

resultats = {an: run_annee(an) for an in ANNEES}
print("\n✔ runs terminés :", list(resultats))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. KPI de tête — 2023 vs 2024
# MAGIC
# MAGIC Une colonne par année, une ligne par indicateur (table `synthese`).

# COMMAND ----------

_KPI = [
    "DATE_INVENTAIRE", "TAUX_CHUTE_PCT", "TAUX_CHUTE_N1_PCT", "CONFORMITE_GLOBALE_PCT",
    "TAUX_COUVERTURE_MRM_PCT", "TAUX_RECUP_GLOBAL_PCT",
    "NB_BASE_CHUTE", "PM_MRM_BASE_CHUTE", "PM_CPT_BASE_CHUTE", "ECART_BASE_CHUTE",
    "NB_MATCHES", "NB_RECUP_N1", "NB_CPT_ONLY",
]

kpi = pd.DataFrame({
    an: resultats[an]["tables"]["synthese"].iloc[0] for an in ANNEES
})
kpi = kpi.loc[[c for c in _KPI if c in kpi.index]]
display(kpi.reset_index().rename(columns={"index": "INDICATEUR"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Taux de chute par ancienneté (N / N-1 / N-2+) — 2023 vs 2024
# MAGIC
# MAGIC Bloc « Inventaire courant » (les stats globales). Le pivot compare le taux
# MAGIC de chute de chaque bloc d'ancienneté entre les deux années.

# COMMAND ----------

def _anc_inv(annee: str) -> pd.DataFrame:
    anc = resultats[annee]["tables"]["chute_par_anciennete"]
    inv = anc[anc["EXERCICE"] == metrics.EXERCICE_INV].copy()
    inv.insert(0, "ANNEE", annee)
    return inv[["ANNEE", "BLOC_ANCIENNETE", "NB_DOSSIERS", "PM_MRM", "PM_CPT",
               "ECART", "TAUX_CHUTE_PCT", "POIDS_PM_PCT"]]

anc_detail = pd.concat([_anc_inv(an) for an in ANNEES], ignore_index=True)
display(anc_detail)

# COMMAND ----------

# Pivot : taux de chute (%) par bloc d'ancienneté × année.
pivot_taux = anc_detail.pivot_table(
    index="BLOC_ANCIENNETE", columns="ANNEE", values="TAUX_CHUTE_PCT", aggfunc="first",
).reindex([metrics.BLOC_N, metrics.BLOC_N1, metrics.BLOC_N2_PLUS, metrics.BLOC_INDET]).dropna(how="all")
display(pivot_taux.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Investigation des orphelins — 2023 vs 2024

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orphelins par type de compte (ventilation complète) par année

# COMMAND ----------

def _orph_type(annee: str) -> pd.DataFrame:
    t = resultats[annee]["tables"]["orphelins_par_type_compte"].copy()
    t.insert(0, "ANNEE", annee)
    return t

display(pd.concat([_orph_type(an) for an in ANNEES], ignore_index=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compte le plus représentatif (RANG 1) par année — détail par clause

# COMMAND ----------

def _top_orphelins(annee: str, n: int = 5) -> pd.DataFrame:
    t = resultats[annee]["tables"]["orphelins_par_clause"].head(n).copy()
    t.insert(0, "ANNEE", annee)
    return t

display(pd.concat([_top_orphelins(an) for an in ANNEES], ignore_index=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orphelins par garantie (volumétrie)

# COMMAND ----------

def _orph_dim(annee: str, table: str, key: str) -> pd.DataFrame:
    t = resultats[annee]["tables"][table][[key, "NB_DOSSIERS", "PM_CPT"]].copy()
    t = t.rename(columns={"NB_DOSSIERS": f"NB_{annee}", "PM_CPT": f"PM_{annee}"})
    return t.set_index(key)

gar = pd.concat([_orph_dim(an, "orphelins_par_garantie", "GARANTIE_LIBELLE") for an in ANNEES], axis=1)
display(gar.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orphelins par ancienneté (volumétrie)

# COMMAND ----------

anc_orph = pd.concat(
    [_orph_dim(an, "orphelins_par_anciennete", "BLOC_ANCIENNETE") for an in ANNEES], axis=1
).reindex([metrics.BLOC_N, metrics.BLOC_N1, metrics.BLOC_N2_PLUS, metrics.BLOC_INDET]).dropna(how="all")
display(anc_orph.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nullité des colonnes constitutives de la clé (explique l'orphelinage)

# COMMAND ----------

def _cles(annee: str) -> pd.DataFrame:
    t = resultats[annee]["tables"]["orphelins_cles_nulles"][["COMPOSANTE", "PCT_NULL"]].copy()
    return t.rename(columns={"PCT_NULL": f"PCT_NULL_{annee}"}).set_index("COMPOSANTE")

display(pd.concat([_cles(an) for an in ANNEES], axis=1).reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Graphiques par année (chute par ancienneté + orphelins par compte)

# COMMAND ----------

import matplotlib.pyplot as plt

for an in ANNEES:
    tb, dd = resultats[an]["tables"], resultats[an]["d"]
    print(f"───────── Inventaire {an} ─────────")
    graph_chute_par_anciennete(tb["chute_par_anciennete"], dd)
    graph_orphelins_par_compte(tb["orphelins_par_clause"], dd)
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Lecture** : un écart de taux de chute sur N-1 entre 2023 et 2024 reflète
# MAGIC l'effet de la revue tête par tête ; un même compte PB en tête des orphelins
# MAGIC sur les deux années pointe un souscripteur à challenger en priorité.
