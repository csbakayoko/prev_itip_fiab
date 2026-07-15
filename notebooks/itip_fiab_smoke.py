# Databricks notebook source
# MAGIC %md
# MAGIC # ITIP-FIAB — Smoke test (tout le pipeline, sans export)
# MAGIC
# MAGIC But : vérifier de bout en bout que tout tourne après une mise à jour du
# MAGIC code — pipeline → synthèse → 20 tables métriques → recoupements → 11
# MAGIC graphiques. Si toutes les cellules passent, le pipeline est bon : lancer
# MAGIC ensuite `itip_fiab_powerbi` pour l'export réel.
# MAGIC
# MAGIC **Aucune table Delta, aucun fichier de métriques** n'est écrit
# MAGIC (`EXPORT_ANALYSES = False`). Seule réserve : `EXPORT_GRAPHS = True` étant
# MAGIC le défaut, l'étape 1 dépose les 11 PNG dans le dossier graphiques sur
# MAGIC DBFS. Pour un run vraiment sans aucune écriture, poser
# MAGIC `EXPORT_GRAPHS = False` dans `config/profile.py`.
# MAGIC
# MAGIC ⚠ Avant de lancer : mettre le Repo à jour + `dbutils.library.restartPython()`.

# COMMAND ----------

from core.runtime import get_spark
from core import metrics
from core.metrics.viz import restituer_graphiques
from main import run

spark = get_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline complet (main.run : load → matching → récupérations → synthèse)
# MAGIC
# MAGIC Ce que `run` écrit est piloté par `config/profile.py` : `EXPORT_ANALYSES`
# MAGIC (défaut False → aucune table ni fichier de métriques) et `EXPORT_GRAPHS`
# MAGIC (défaut True → les 11 PNG sur DBFS).

# COMMAND ----------

df_result = run(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Contrôles bloquants : synthèse, 20 tables, recoupements inter-tables

# COMMAND ----------

from core.synthese.kpi_export import compute_synthese

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
# MAGIC ## 3. Les 11 graphiques (affichés, non écrits)

# COMMAND ----------

figs = restituer_graphiques(df_result, d, save_dir=None)
print(f"✔ {len(figs)} graphiques rendus")

# COMMAND ----------

print("✅ SMOKE TEST OK — pipeline, synthèse, 20 tables, recoupements et 11 graphiques.")
df_result.unpersist()
