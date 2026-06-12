# Databricks notebook source
# MAGIC %md
# MAGIC # ITIP-FIAB — Smoke test (tout le pipeline, rien d'écrit)
# MAGIC
# MAGIC But : vérifier de bout en bout que tout tourne après un `git pull` —
# MAGIC pipeline → synthèse → 15 tables métriques → recoupements → 9 graphiques.
# MAGIC **Aucune écriture** (ni DBFS, ni Delta) : si toutes les cellules passent,
# MAGIC le pipeline est bon, lancer ensuite `itip_fiab_powerbi` pour l'export.
# MAGIC
# MAGIC ⚠ Avant de lancer : git pull du Repo + `dbutils.library.restartPython()`.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("itip_fiab").getOrCreate()
# AQE + skew join : critique pour les theta-joins des étapes windowed.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

from main import run
from core import metrics
from core.viz import restituer_graphiques

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline complet (main.run : load → matching → récupérations → synthèse)
# MAGIC
# MAGIC Les exports/graphiques de `run` restent pilotés par `config/profile.py`
# MAGIC (EXPORT_ANALYSES / EXPORT_GRAPHS, défaut False) : ici rien n'est écrit.

# COMMAND ----------

df_result = run(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Contrôles bloquants : synthèse, 15 tables, recoupements inter-tables

# COMMAND ----------

from core.kpi_export import compute_synthese

d = compute_synthese(df_result)
assert d["coherent"], f"Lignes non classées : {d['labels_inconnus']}"
assert d["chute_coherente"], "Taux de chute ≠ Σ consignes + hors consigne (voir logs)"

tables = metrics.toutes_metriques(df_result, d)
for name, pdf in tables.items():
    print(f"  ✓ {name:<22} {len(pdf):>4} ligne(s)")

ctrl = tables["controles_coherence"]
assert ctrl["OK"].all(), f"{int((~ctrl['OK']).sum())} recoupement(s) inter-tables KO :\n{ctrl[~ctrl['OK']]}"
print(f"\n✔ recoupements inter-tables : {len(ctrl)}/{len(ctrl)} OK")

display(tables["bilan_cas"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Les 9 graphiques (affichés, non écrits)

# COMMAND ----------

figs = restituer_graphiques(df_result, d, save_dir=None)
print(f"✔ {len(figs)} graphiques rendus")

# COMMAND ----------

print("✅ SMOKE TEST OK — pipeline, synthèse, 15 tables, recoupements et 9 graphiques.")
df_result.unpersist()
