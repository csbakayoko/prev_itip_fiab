# Databricks notebook source
# MAGIC %md
# MAGIC # ITIP-FIAB — Run de production → Power BI
# MAGIC
# MAGIC Notebook **final** : données + config en entrée, tables métriques en sortie.
# MAGIC
# MAGIC Déroulé :
# MAGIC 1. **Setup** — session Spark + config (fichiers source et périmètre pilotés par `config/profile.py`)
# MAGIC 2. **Pipeline** — chargement → nettoyage → matching → récupérations (N+1, statut NON) → tags
# MAGIC 3. **Synthèse** — contrôles de cohérence console (`chute_coherente`, lignes classées)
# MAGIC 4. **Export Power BI** — les 11 tables métriques écrites en Delta (+ fichiers DBFS)
# MAGIC
# MAGIC Tables produites (tidy, une table par question métier) :
# MAGIC
# MAGIC | Table | Contenu |
# MAGIC |---|---|
# MAGIC | `synthese` | 1 ligne / run — tous les KPI (chute, conformité, couvertures, retrouvés vs base chute) — **historisable** |
# MAGIC | `taux_chute_global` | le taux + ses composantes PM (base chute, retrouvés, totaux) |
# MAGIC | `consignes` | conformité + PM + chute, 1 ligne / consigne |
# MAGIC | `compte_justification` | décomposition du compte (retrouvés, N+1, repêchés, clos, anomalies) |
# MAGIC | `couverture_mrm` | part de la revue retrouvée au compte + non retrouvés par consigne |
# MAGIC | `chute_par_clause` | taux de chute ventilé par CLAUSE × TYPE_CLAUSE |
# MAGIC | `chute_par_consigne` / `pm_par_consigne` | chute et PM par consigne pertinente |
# MAGIC | `conformite_consignes` / `conformite_globale` | application des consignes (détail + segments) |
# MAGIC | `anomalies_cpt_only` | anomalies par mois de survenance (effet fin d'année) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup — session Spark + config
# MAGIC
# MAGIC Le notebook vit dans le repo (dossier Git Databricks) : la racine est sur `sys.path`.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("itip_fiab").getOrCreate()
# AQE + skew join : critique pour les theta-joins des étapes windowed.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

from config import (
    db_cfg, tech_cfg, RUN_PARAMS, CLIENT_NAME, RECUP_NON_LABEL, CHECKPOINT_DIR,
    EXPORT_DELTA_SCHEMA,
)
from modules.load_data import load_cpt_raw, load_mrm_raw
from modules.transform import clean_cpt, clean_mrm
from modules.matching import (
    matching_waterfall, recover_late_declarations,
    flag_late_it_observations, enrich_result_tags,
)
from modules.kpi_export import print_synthese
from modules import metrics
from main import _split_mrm_statut

if CHECKPOINT_DIR:
    spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

# ── Cible de l'export Power BI ───────────────────────────────────────────────
# Delta (recommandé) : Power BI se connecte au SQL Warehouse Databricks et lit
# les tables <schema>.itip_metric_<nom>_<perim>. Sans schéma, les fichiers
# parquet/csv sous DBFS restent disponibles (connecteur fichier / import).
DELTA_SCHEMA = EXPORT_DELTA_SCHEMA            # ex. "hive_metastore.itip_fiab"
FORMATS      = ("delta", "parquet", "csv") if DELTA_SCHEMA else ("parquet", "csv")

print("Périmètre :", CLIENT_NAME)
print("Formats   :", FORMATS, "| schéma Delta :", DELTA_SCHEMA or "—")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline — construction de `df_result`
# MAGIC
# MAGIC Identique à `main.run` : matching principal (MRM statut OUI), récupération
# MAGIC N+1, repêchage statut NON (hors métriques), obs tardives IT, tags persistants.

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
# MAGIC ## 3. Synthèse + contrôles de cohérence
# MAGIC
# MAGIC `print_synthese` renvoie `d` (une seule passe Spark, réutilisée par toutes
# MAGIC les métriques). **Vérifier avant export** : `✔ cohérent` sur les lignes
# MAGIC classées, le contrôle Σ consignes du taux de chute et l'hypothèse PM MRM = 0
# MAGIC des repêchés statut NON.

# COMMAND ----------

d = print_synthese(df_result)

assert d["coherent"], (
    f"Lignes non classées : {d['labels_inconnus']} — export interrompu."
)
assert d["chute_coherente"], (
    "Taux de chute global ≠ Σ consignes + hors consigne — export interrompu (voir logs)."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Export Power BI
# MAGIC
# MAGIC Une passe : les 11 tables métriques écrites en Delta + parquet/csv sur DBFS.

# COMMAND ----------

tables = metrics.export_metriques(
    df_result, d,
    formats      = FORMATS,
    delta_schema = DELTA_SCHEMA,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Récapitulatif des sorties (connexion Power BI)

# COMMAND ----------

out_dir = metrics.output_dir(sub="metrics")
print(f"Fichiers DBFS : {out_dir}\n")
if DELTA_SCHEMA:
    print("Tables Delta (connecteur Power BI ▸ Azure Databricks ▸ SQL Warehouse) :")
    for name in tables:
        print(f"  {DELTA_SCHEMA}.itip_metric_{name}_{metrics._PERIMETRE}")
else:
    print("Pas de schéma Delta configuré (EXPORT_DELTA_SCHEMA dans config/profile.py)")
    print("→ Power BI : importer les parquet/csv du dossier ci-dessus.")

# Aperçu de la ligne de synthèse exportée (KPI du run).
display(tables["synthese"])

# COMMAND ----------

df_result.unpersist()
