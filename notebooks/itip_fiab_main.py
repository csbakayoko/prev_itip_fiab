# Databricks notebook source
# MAGIC %md
# MAGIC # ITIP-FIAB — Notebook principal (run par année d'inventaire)
# MAGIC
# MAGIC Pipeline de fiabilisation CPT/MRM, puis **couche métriques** (`core.metrics`).
# MAGIC
# MAGIC Déroulé :
# MAGIC 1. Setup (Spark) + **widgets de run** (année d'inventaire 2023 / 2024)
# MAGIC 2. Construction de `df_result` (`main.build_df_result`)
# MAGIC 3. Synthèse console (rappel)
# MAGIC 4. **Métriques** : une fonction par indicateur, affichées en table
# MAGIC    (dont chute par ancienneté + investigation des orphelins)
# MAGIC 5. **Export** des métriques (CSV / JSON / Parquet) sur DBFS
# MAGIC 6. Graphiques de restitution *(optionnel)*
# MAGIC
# MAGIC L'**année d'inventaire** se choisit via le widget en tête (2023 / 2024) ;
# MAGIC les fichiers MRM, la vision CPT et la date sont des widgets éditables.
# MAGIC Pour comparer les deux années côte à côte : `itip_fiab_comparaison`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup — session Spark + widgets de run
# MAGIC
# MAGIC Le notebook vit dans le repo (dossier Git Databricks) : la racine du repo est
# MAGIC **automatiquement** sur `sys.path`, aucun chemin à ajouter.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("itip_fiab").getOrCreate()
# AQE + skew join : critique pour les theta-joins des étapes windowed.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

from main import build_df_result
from core.runtime import configurer_run
from core.synthese.kpi_export import print_synthese
from core import metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets — année d'inventaire + sources
# MAGIC
# MAGIC `annee_inventaire` sélectionne le run (2023 / 2024). Les valeurs 2023 sont
# MAGIC pré-remplies (cf. `profile.py`) ; **renseigner les chemins MRM 2024** (et la
# MAGIC vision CC2024) avant de lancer le run 2024.

# COMMAND ----------

dbutils.widgets.dropdown("annee_inventaire", "2023", ["2023", "2024"], "Année d'inventaire")

# 2023 — valeurs connues (alignées sur config/profile.py)
dbutils.widgets.text("date_2023", "31/12/2023", "2023 · date d'inventaire")
dbutils.widgets.text("vision_2023", "CC2023", "2023 · vision CPT")
dbutils.widgets.text(
    "mrm_2023",
    "dbfs:/FileStore/shared_uploads/cheickseko.bakayoko@axa.fr/MRM_FILES/MRM_Fiab_31_12_23_V3.csv",
    "2023 · MRM courant",
)
dbutils.widgets.text(
    "mrm_2023_n1",
    "dbfs:/FileStore/shared_uploads/cheickseko.bakayoko@axa.fr/MRM_FILES/MRM_Fiab_30_06_24.csv",
    "2023 · MRM N+1",
)

# 2024 — vision CC2024 ; chemins MRM à renseigner (placeholders à ajuster).
dbutils.widgets.text("date_2024", "31/12/2024", "2024 · date d'inventaire")
dbutils.widgets.text("vision_2024", "CC2024", "2024 · vision CPT")
dbutils.widgets.text(
    "mrm_2024",
    "dbfs:/FileStore/shared_uploads/cheickseko.bakayoko@axa.fr/MRM_FILES/MRM_Fiab_31_12_24.csv",
    "2024 · MRM courant (À AJUSTER)",
)
dbutils.widgets.text("mrm_2024_n1", "", "2024 · MRM N+1 (option)")

# COMMAND ----------

# Applique la config de l'année choisie AVANT toute construction.
annee  = dbutils.widgets.get("annee_inventaire")
profil = configurer_run(
    date_inventaire=dbutils.widgets.get(f"date_{annee}"),
    cpt_vision     =dbutils.widgets.get(f"vision_{annee}"),
    fichier_mrm    =dbutils.widgets.get(f"mrm_{annee}"),
    fichier_mrm_n1 =dbutils.widgets.get(f"mrm_{annee}_n1") or None,
)
print(f"Run inventaire {annee} :", profil)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Construction de `df_result`
# MAGIC
# MAGIC `main.build_df_result` : chargement → matching → récupération N+1 →
# MAGIC repêchage statut NON → obs tardives IT → tags. Mêmes étapes que `main.run`,
# MAGIC sans la restitution (pilotée ici cellule par cellule).

# COMMAND ----------

df_result = build_df_result(spark).persist()
print("df_result :", df_result.count(), "lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Synthèse console (rappel)
# MAGIC
# MAGIC `print_synthese` renvoie `d`, le dict de `compute_synthese` : la passe
# MAGIC Spark est faite **une seule fois**, réutilisée par toutes les métriques.

# COMMAND ----------

d = print_synthese(df_result)
annee_inv = metrics._annee_inventaire(d)   # année dérivée de d["date_inventaire"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Métriques
# MAGIC
# MAGIC Des fonctions simples (`core.metrics`) : les métriques scalaires prennent
# MAGIC `d` ; `chute_par_*`, `anomalies_cpt_only` et `orphelins_*` ré-agrègent
# MAGIC `df_result` côté Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Synthèse (1 ligne) + bilan cas par cas + taux de chute

# COMMAND ----------

display(metrics.synthese(d))

# COMMAND ----------

display(metrics.bilan_cas(d))   # LE bilan cas par cas (avec explications)

# COMMAND ----------

display(metrics.taux_chute(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chute par exercice (inventaire courant / N+1 séparé) + suivi consignes N+1

# COMMAND ----------

display(metrics.chute_par_exercice(d))

# COMMAND ----------

display(metrics.suivi_n1(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chute par ancienneté — N / N-1 / N-2 et antérieur
# MAGIC
# MAGIC La méthode d'inventaire diffère selon l'année de survenance (revue tête par
# MAGIC tête sur N-1). Σ du bloc « Inventaire courant » = taux de chute principal.

# COMMAND ----------

display(metrics.chute_par_anciennete(df_result, annee_inv))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyse des consignes (conformité + PM + chute) — exercice courant pur

# COMMAND ----------

display(metrics.consignes(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Investigation des orphelins (CPT_ONLY, compte préposé)
# MAGIC
# MAGIC `orphelins_par_clause` : RANG 1 = compte PB le plus représentatif (à
# MAGIC investiguer avec le souscripteur). `orphelins_cles_nulles` : composantes de
# MAGIC la clé nulles/vides (explique pourquoi ces dossiers n'ont pas matché).

# COMMAND ----------

display(metrics.orphelins_par_clause(df_result))      # compte PB le plus représentatif (RANG 1)

# COMMAND ----------

display(metrics.orphelins_par_garantie(df_result))    # IT 60 / IP 64 / autre

# COMMAND ----------

display(metrics.orphelins_par_anciennete(df_result, annee_inv))

# COMMAND ----------

display(metrics.orphelins_cles_nulles(df_result))     # nullité des colonnes de la clé

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autres données derrière les graphiques

# COMMAND ----------

display(metrics.compte_justification(d))            # graphe 1
display(metrics.couverture_mrm(d))                  # graphe 2
display(metrics.chute_par_clause(df_result, top=12))  # graphe 3 — top 12 par bloc EXERCICE
display(metrics.chute_par_consigne(d))              # graphe 4
display(metrics.conformite_consignes(d))            # graphe 5
display(metrics.anomalies_cpt_only(df_result))      # graphe 6
display(metrics.conformite_globale(d))              # graphe 8
display(metrics.pm_par_consigne(d))                 # graphe 9

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Export des métriques (CSV / JSON / Parquet) sur DBFS
# MAGIC
# MAGIC Dossier `.../<PERIMETRE>/metrics`. Une métrique = 3 fichiers (un par format).
# MAGIC ⚠ Le nom d'export n'encode PAS l'année : pour historiser 2023 ET 2024 sans
# MAGIC écrasement, ajuster `CLIENT_NAME` dans `profile.py` par run.

# COMMAND ----------

_ = metrics.export_metriques(df_result, d, formats=("csv", "json", "parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Graphiques de restitution *(optionnel)*
# MAGIC
# MAGIC Restitution matplotlib (affichage + PNG DBFS), 11 graphiques (dont chute par
# MAGIC ancienneté et orphelins par compte). Décommentez pour lancer.

# COMMAND ----------

# from core.metrics.viz import restituer_graphiques
# figs = restituer_graphiques(df_result, d)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC `df_result` reste en cache pour vos explorations ad hoc. Pour libérer : `df_result.unpersist()`.
