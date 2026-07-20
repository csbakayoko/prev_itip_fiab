# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 ITIP-FIAB — Smoke test (tout le pipeline, sans aucune écriture)
# MAGIC
# MAGIC But : vérifier de bout en bout que tout tourne après une mise à jour du
# MAGIC code — pipeline → synthèse → 8 tables métriques (contrôles inclus) →
# MAGIC recoupements → 11 graphiques. Si toutes les cellules passent, le pipeline
# MAGIC est bon : lancer ensuite 🚀 `itip_fiab_powerbi` pour l'export réel.
# MAGIC
# MAGIC **Aucune écriture** : ni table Delta, ni fichier DBFS, ni PNG. Ce
# MAGIC notebook n'appelle donc PAS `main.run` (qui, lui, exporte : le Hive est
# MAGIC la sortie de référence du pipeline) mais `main.build_df_result`, le cœur
# MAGIC métier seul — puis il rejoue la restitution en mémoire.
# MAGIC
# MAGIC ⚠ Avant de lancer : mettre le Repo à jour + `dbutils.library.restartPython()`.

# COMMAND ----------

from core.runtime import get_spark
from core import metrics
from core.metrics.viz import restituer_graphiques
from core.synthese.kpi_export import print_synthese
from main import build_df_result

spark = get_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline (build_df_result : load → matching → récupérations → tags)
# MAGIC
# MAGIC Le cœur métier seul, sans restitution ni export — rien n'est écrit.

# COMMAND ----------

df_result = build_df_result(spark).persist()
print("df_result :", df_result.count(), "lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Contrôles bloquants : synthèse, 8 tables, recoupements inter-tables
# MAGIC
# MAGIC `print_synthese` renvoie `d` (une seule passe Spark, réutilisée ensuite).
# MAGIC `toutes_metriques` calcule les tables **en mémoire**, sans les exporter.

# COMMAND ----------

d = print_synthese(df_result)
assert d["coherent"], f"Lignes non classées : {d['labels_inconnus']}"
assert d["chute_coherente"], "Taux de chute ≠ Σ consignes + hors consigne (voir logs)"

tables = metrics.toutes_metriques(df_result, d)
for name, pdf in tables.items():
    print(f"  ✓ {name:<28} {len(pdf):>4} ligne(s)")

ctrl = tables["controles_coherence"]
assert ctrl["OK"].all(), f"{int((~ctrl['OK']).sum())} recoupement(s) inter-tables KO :\n{ctrl[~ctrl['OK']]}"
print(f"\n✔ recoupements inter-tables : {len(ctrl)}/{len(ctrl)} OK")

display(tables["bilan_cas"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Les 11 graphiques (affichés, non écrits)
# MAGIC
# MAGIC `save_dir=None` : rendu en mémoire, aucun PNG déposé sur DBFS.

# COMMAND ----------

figs = restituer_graphiques(df_result, d, save_dir=None)
print(f"✔ {len(figs)} graphiques rendus")

# COMMAND ----------

print("✅ SMOKE TEST OK — pipeline, synthèse, 8 tables, recoupements et 11 graphiques.")
print("   Aucune écriture. Pour exporter : lancer itip_fiab_powerbi.")
df_result.unpersist()
