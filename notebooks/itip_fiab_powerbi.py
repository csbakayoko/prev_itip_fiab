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
# MAGIC 4. **Export Power BI** — les 15 tables métriques écrites en Delta (+ fichiers DBFS),
# MAGIC    puis contrôles inter-tables BLOQUANTS (les onglets doivent se recouper)
# MAGIC
# MAGIC Tables produites (tidy, une table par question métier) :
# MAGIC
# MAGIC | Table | Contenu |
# MAGIC |---|---|
# MAGIC | `synthese` | 1 ligne / run — tous les KPI (chute, conformité, couvertures, retrouvés vs base chute) — **historisable** |
# MAGIC | `bilan_cas` | LE bilan cas par cas : matchés, retrouvés par tentatives (N+1, statut NON), non retrouvés de part et d'autre — nb, PM, taux, **explication** |
# MAGIC | `taux_chute` | LE taux (matchés inventaire courant) + composantes PM (base chute, retrouvés, totaux) ; N+1 en regard |
# MAGIC | `chute_par_exercice` | 1 ligne / exercice (inventaire courant, N+1 séparé) — nb, PM, écart, taux |
# MAGIC | `suivi_n1` | consignes des récupérés N+1 (analyse séparée), 1 ligne / consigne N+1 |
# MAGIC | `consignes` | conformité + PM + chute (exercice courant pur), 1 ligne / consigne |
# MAGIC | `compte_justification` | décomposition du compte (retrouvés, N+1, repêchés, clos, anomalies) |
# MAGIC | `couverture_mrm` | part de la revue retrouvée au compte + non retrouvés par consigne |
# MAGIC | `chute_par_clause` | taux de chute par CLAUSE × TYPE_CLAUSE × EXERCICE (inventaire courant / N+1 séparé) |
# MAGIC | `chute_par_consigne` / `pm_par_consigne` | chute et PM par consigne pertinente |
# MAGIC | `conformite_consignes` / `conformite_globale` | application des consignes (détail + segments) |
# MAGIC | `anomalies_cpt_only` | anomalies par mois de survenance (effet fin d'année) |
# MAGIC | `controles_coherence` | recoupements inter-tables (attendu / obtenu / OK) — onglet « fiabilité » |

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
from core.load_data import load_cpt_raw, load_mrm_raw
from core.transform import clean_cpt, clean_mrm
from core.matching import (
    matching_waterfall, recover_late_declarations,
    flag_late_it_observations, enrich_result_tags,
)
from core.kpi_export import print_synthese
from core import metrics
from main import _split_mrm_statut

if CHECKPOINT_DIR:
    spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

# ── Cible de l'export Power BI ───────────────────────────────────────────────
# Delta (recommandé) : Power BI se connecte au SQL Warehouse Databricks et lit
# les tables <schema>.itip_metric_<nom>_<perim>. Sans schéma, les fichiers
# parquet/csv sous DBFS restent disponibles (connecteur fichier / import).
DELTA_SCHEMA = EXPORT_DELTA_SCHEMA            # ex. "hive_metastore.itip_fiab"
# Excel TOUJOURS produit (classeur propre multi-onglets, import fichier Power BI) ;
# Delta en plus si un schéma est configuré (connexion SQL Warehouse).
FORMATS      = ("excel", "delta", "parquet", "csv") if DELTA_SCHEMA else ("excel", "parquet", "csv")

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
    "Taux de chute ≠ Σ consignes + hors consigne — export interrompu (voir logs)."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Export Power BI
# MAGIC
# MAGIC Une passe : les 15 tables métriques écrites en Delta + parquet/csv sur DBFS.

# COMMAND ----------

tables = metrics.export_metriques(
    df_result, d,
    formats      = FORMATS,
    delta_schema = DELTA_SCHEMA,
)

# Les onglets Power BI doivent se recouper : tout contrôle inter-tables KO
# invalide l'étude → run en échec (la table controles_coherence est exportée,
# à afficher en onglet « fiabilité » dans Power BI).
ctrl = tables["controles_coherence"]
display(ctrl)
assert ctrl["OK"].all(), (
    f"{int((~ctrl['OK']).sum())} contrôle(s) inter-tables KO — onglets Power BI "
    "incohérents (voir la table controles_coherence ci-dessus)."
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
    print("→ Power BI : importer le classeur Excel (onglets par axe + Sommaire) ou les parquet/csv du dossier ci-dessus.")

# Aperçu de la ligne de synthèse exportée (KPI du run).
display(tables["synthese"])

# COMMAND ----------

df_result.unpersist()
