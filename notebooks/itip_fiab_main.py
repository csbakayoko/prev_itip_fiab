# Databricks notebook source
# MAGIC %md
# MAGIC # ITIP-FIAB — Notebook principal
# MAGIC
# MAGIC Pipeline de fiabilisation CPT/MRM, puis **couche métriques** (`core.metrics`).
# MAGIC
# MAGIC Déroulé :
# MAGIC 1. Setup (Spark + config)
# MAGIC 2. Construction de `df_result` (chargement → matching → récupérations → enrichissement)
# MAGIC 3. Synthèse console (rappel)
# MAGIC 4. **Métriques** : une fonction par indicateur, affichées en table
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
from core.io.load_data import load_cpt_raw, load_mrm_raw
from core.prep.transform import clean_cpt, clean_mrm
from core.match.matching import (
    matching_waterfall, recover_late_declarations,
    flag_late_it_observations, enrich_result_tags,
)
from core.synthese.kpi_export import print_synthese
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
# Seuls les OUI du N+1 → CPT_LATE (dans les métriques) ; les NON du N+1 → passe
# statut NON (CPT_RECUP_NON, hors métriques).
mrm_n1_non = None
if RUN_PARAMS.get("fichier_mrm_n1"):
    mrm_n1 = clean_mrm(load_mrm_raw(spark, db_cfg, "fichier_mrm_n1"), tech_cfg)
    mrm_n1_oui, mrm_n1_non = _split_mrm_statut(mrm_n1)
    df_result = recover_late_declarations(df_result, [("MRM_N1", mrm_n1_oui)])

# Repêchage via statut NON (→ CPT_RECUP_NON, hors métriques) sur les DEUX
# exercices, LATE_SOURCE distinct (STATUT_NON / STATUT_NON_N1) pour ventiler la part.
non_inventories = [("STATUT_NON", mrm_non)]
if mrm_n1_non is not None:
    non_inventories.append(("STATUT_NON_N1", mrm_n1_non))
df_result = recover_late_declarations(
    df_result, non_inventories, label=RECUP_NON_LABEL,
)

# Obs tardives IT (anomalies, jamais matchées) + tags persistants.
df_result = flag_late_it_observations(df_result)
df_result = enrich_result_tags(df_result)

df_result = df_result.persist()
print("df_result :", df_result.count(), "lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Synthèse console (rappel)
# MAGIC
# MAGIC `print_synthese` renvoie `d`, le dict de `compute_synthese` : la passe
# MAGIC Spark est faite **une seule fois**, réutilisée par toutes les métriques.

# COMMAND ----------

d = print_synthese(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Métriques
# MAGIC
# MAGIC Des fonctions simples (`core.metrics`) : les métriques scalaires
# MAGIC prennent `d` et renvoient un DataFrame pandas ; `chute_par_clause` et
# MAGIC `anomalies_cpt_only` ré-agrègent `df_result` côté Spark.

# COMMAND ----------

from core import metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Synthèse (1 ligne) + taux de chute (PM MRM / PM Compte)

# COMMAND ----------

display(metrics.synthese(d))

# COMMAND ----------

display(metrics.bilan_cas(d))   # LE bilan cas par cas (avec explications)

# COMMAND ----------

display(metrics.taux_chute(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chute par exercice (inventaire courant / N+1 séparé) + suivi des consignes N+1

# COMMAND ----------

display(metrics.chute_par_exercice(d))

# COMMAND ----------

display(metrics.suivi_n1(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyse des consignes (conformité + PM + chute) — exercice courant pur

# COMMAND ----------

display(metrics.consignes(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Données derrière les graphiques

# COMMAND ----------

display(metrics.compte_justification(d))            # graphe 1

# COMMAND ----------

display(metrics.couverture_mrm(d))                  # graphe 2

# COMMAND ----------

display(metrics.chute_par_clause(df_result, top=12))  # graphe 3 — top 12 par bloc EXERCICE

# COMMAND ----------

display(metrics.chute_par_consigne(d))              # graphe 4

# COMMAND ----------

display(metrics.conformite_consignes(d))            # graphe 5

# COMMAND ----------

display(metrics.anomalies_cpt_only(df_result))      # graphe 6

# COMMAND ----------

display(metrics.conformite_globale(d))              # graphe 8

# COMMAND ----------

display(metrics.pm_par_consigne(d))                 # graphe 9

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Export des métriques (CSV / JSON / Parquet) sur DBFS
# MAGIC
# MAGIC Dossier `.../<PERIMETRE>/metrics`. Une métrique = 3 fichiers (un par format).

# COMMAND ----------

_ = metrics.export_metriques(df_result, d, formats=("csv", "json", "parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Graphiques de restitution *(optionnel)*
# MAGIC
# MAGIC Restitution matplotlib (affichage + PNG DBFS), depuis la même passe métriques. Décommentez pour lancer.

# COMMAND ----------

# from core.metrics.viz import restituer_graphiques
# figs = restituer_graphiques(df_result, d)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC `df_result` reste en cache pour vos explorations ad hoc. Pour libérer : `df_result.unpersist()`.
