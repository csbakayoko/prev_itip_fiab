# Databricks notebook source
# MAGIC %md
# MAGIC # Investigations orphelins ITIP-FIAB
# MAGIC
# MAGIC Croisement des orphelins (`CPT_ONLY` / `MRM_MISSING`) avec l'entrepôt de
# MAGIC données (gros Excel multi-inventaire) pour tracer leur historique.
# MAGIC
# MAGIC Prérequis :
# MAGIC - lib **spark-excel** (`com.crealytics:spark-excel`) installée sur le cluster ;
# MAGIC - `FICHIER_ENTREPOT_CPT` / `FICHIER_ENTREPOT_MRM` renseignés dans `config/profile.py`.

# COMMAND ----------

from main import run
from config import RUN_PARAMS
from investigations.orphans import extract_orphans
from investigations.warehouse import load_warehouse_excel, prepare_warehouse
from investigations.analyze import trace_history, history_stats, print_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Résultat de réconciliation (orphelins inclus)

# COMMAND ----------

df_result = run(spark)  # noqa: F821 (spark fourni par Databricks)
df_result.groupBy("TYPE_RECONCILIATION").count().orderBy("TYPE_RECONCILIATION").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Choix de la base à investiguer
# MAGIC `"cpt"` → CPT_ONLY vs entrepôt CPT · `"mrm"` → MRM_MISSING vs entrepôt MRM.

# COMMAND ----------

base = "mrm"   # ← changer en "cpt" pour l'autre base
path = RUN_PARAMS["entrepot_" + base]

orphans   = extract_orphans(df_result, base)
warehouse = prepare_warehouse(load_warehouse_excel(spark, path), base)  # noqa: F821

print(f"orphelins {base} : {orphans.count():,}")
orphans.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Traçage de l'historique
# MAGIC 1 ligne par (orphelin × apparition entrepôt). `key="key_no_garantie"` pour
# MAGIC suivre un passage IT→IP.

# COMMAND ----------

traced = trace_history(orphans, warehouse, key="key_no_date").cache()
traced.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Statistiques par dossier + synthèse

# COMMAND ----------

stats = history_stats(traced, base, key="key_no_date").cache()
print_summary(stats, base)
stats.orderBy("retrouve", "n_apparitions").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pistes d'analyse
# MAGIC - **Non retrouvés** (`retrouve = false`) : dossiers absents de l'entrepôt → vrais orphelins.
# MAGIC - **Dérive PM/PSAP** (`pm_drift`, `psap_drift`) : évolution entre 1re et dernière apparition.
# MAGIC - **Apparitions tardives** (`inv_max` > inventaire de référence) : candidats déclarations tardives.

# COMMAND ----------

# Exemple : orphelins jamais retrouvés dans l'entrepôt
stats.filter("not retrouve").show(50, truncate=False)
