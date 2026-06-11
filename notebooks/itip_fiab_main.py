# Databricks notebook source
# MAGIC %md
# MAGIC # ITIP-FIAB — Notebook principal
# MAGIC
# MAGIC Pipeline de fiabilisation CPT/MRM, puis **couche métriques** (`modules.metrics.Metrics`).
# MAGIC
# MAGIC Déroulé :
# MAGIC 1. Setup (Spark + config)
# MAGIC 2. Construction de `df_result` (chargement → matching → récupérations → enrichissement)
# MAGIC 3. Synthèse console (rappel)
# MAGIC 4. **Métriques** : une méthode par indicateur, affichées en table
# MAGIC 5. **Export** des métriques (CSV / JSON / Parquet) sur DBFS
# MAGIC 6. Graphiques de restitution *(optionnel)*
# MAGIC
# MAGIC Le périmètre, les fichiers source et les options sont pilotés par `config/profile.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup — session Spark + config
# MAGIC
# MAGIC Le notebook vit dans le repo (dossier Git Databricks) : la racine du repo est
# MAGIC **automatiquement** sur `sys.path`, aucun chemin à ajouter.
# MAGIC
# MAGIC Hors dossier Git uniquement, installer le projet à la place :
# MAGIC `%pip install -e /Workspace/Repos/<vous>/prev_itip_fiab` (cf. `pyproject.toml`).

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("itip_fiab").getOrCreate()
# AQE + skew join : critique pour les theta-joins des étapes windowed.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

from config import (
    db_cfg, tech_cfg, RUN_PARAMS, CLIENT_NAME, RECUP_NON_LABEL, CHECKPOINT_DIR,
)
from modules.load_data import load_cpt_raw, load_mrm_raw
from modules.transform import clean_cpt, clean_mrm
from modules.matching import (
    matching_waterfall, recover_late_declarations,
    flag_late_it_observations, enrich_result_tags,
)
from modules.kpi_export import print_synthese
from main import _split_mrm_statut

if CHECKPOINT_DIR:
    spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

print("Périmètre :", CLIENT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Construction de `df_result`
# MAGIC
# MAGIC Reproduit fidèlement `main.run` (sans la restitution) : matching principal,
# MAGIC récupération N+1, repêchage statut NON, obs tardives IT, tags.

# COMMAND ----------

cpt_clean = clean_cpt(load_cpt_raw(spark, db_cfg), tech_cfg)
mrm_clean = clean_mrm(load_mrm_raw(spark, db_cfg), tech_cfg)

# Statut NON réservé au repêchage des CPT_ONLY (PM MRM = 0).
mrm_oui, mrm_non = _split_mrm_statut(mrm_clean)

df_result = matching_waterfall(cpt_clean, mrm_oui)

# Déclarations tardives : CPT_ONLY retrouvés dans l'inventaire MRM N+1 (→ CPT_LATE).
if RUN_PARAMS.get("fichier_mrm_n1"):
    mrm_n1 = clean_mrm(load_mrm_raw(spark, db_cfg, "fichier_mrm_n1"), tech_cfg)
    mrm_n1_oui, _ = _split_mrm_statut(mrm_n1)
    df_result = recover_late_declarations(df_result, [("MRM_N1", mrm_n1_oui)])

# Repêchage via statut NON (→ CPT_RECUP_NON, hors métriques).
df_result = recover_late_declarations(
    df_result, [("STATUT_NON", mrm_non)], label=RECUP_NON_LABEL,
)

# Obs tardives IT (anomalies, jamais matchées) + tags persistants.
df_result = flag_late_it_observations(df_result)
df_result = enrich_result_tags(df_result)

df_result = df_result.persist()
print("df_result :", df_result.count(), "lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Synthèse console (rappel)

# COMMAND ----------

_ = print_synthese(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Métriques
# MAGIC
# MAGIC `Metrics(df_result)` calcule `compute_synthese` **une seule fois** ;
# MAGIC chaque méthode renvoie un `Metric` sérialisable
# MAGIC (`.to_pandas()` / `.to_spark()` / `.to_json()` / `.to_csv()` / `.to_parquet()`).

# COMMAND ----------

from modules.metrics import Metrics

m = Metrics(df_result)   # une passe Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Synthèse (1 ligne) + taux de chute global (PM MRM / PM Compte)

# COMMAND ----------

display(m.synthese().to_pandas())

# COMMAND ----------

display(m.taux_chute_global().to_pandas())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyse des consignes (conformité + PM + chute)

# COMMAND ----------

display(m.consignes().to_pandas())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Données derrière les graphiques

# COMMAND ----------

display(m.compte_justification().to_pandas())   # graphe 1

# COMMAND ----------

display(m.couverture_mrm().to_pandas())          # graphe 2

# COMMAND ----------

display(m.chute_par_clause(top=12).to_pandas())  # graphe 3

# COMMAND ----------

display(m.chute_par_consigne().to_pandas())      # graphe 4

# COMMAND ----------

display(m.conformite_consignes().to_pandas())    # graphe 5

# COMMAND ----------

display(m.anomalies_cpt_only().to_pandas())      # graphe 6

# COMMAND ----------

display(m.conformite_globale().to_pandas())      # graphe 8

# COMMAND ----------

display(m.pm_par_consigne().to_pandas())         # graphe 9

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exemples de sérialisation
# MAGIC Chaque métrique se sort aussi en JSON (str), Spark DataFrame, etc.

# COMMAND ----------

print(m.taux_chute_global().to_json())
# m.consignes().to_spark().show(truncate=False)   # version Spark DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Export des métriques (CSV / JSON / Parquet) sur DBFS
# MAGIC
# MAGIC Dossier `.../<PERIMETRE>/metrics`. Une métrique = 3 fichiers (un par format).

# COMMAND ----------

_ = m.export(formats=("csv", "json", "parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Graphiques de restitution *(optionnel)*
# MAGIC
# MAGIC Restitution matplotlib (affichage + PNG DBFS), depuis la même passe métriques. Décommentez pour lancer.

# COMMAND ----------

# from modules.viz import restituer_graphiques
# figs = restituer_graphiques(m)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC `df_result` reste en cache pour vos explorations ad hoc. Pour libérer : `df_result.unpersist()`.
