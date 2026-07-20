# Databricks notebook source
# MAGIC %md
# MAGIC # 📓 ITIP-FIAB — Notebook principal (exploration par année d'inventaire)
# MAGIC
# MAGIC Pipeline de fiabilisation CPT/MRM, puis **couche métriques** (`core.metrics`),
# MAGIC déroulés **cellule par cellule** — l'atelier d'exploration et de recette.
# MAGIC Le run de production (Job) est 🚀 `itip_fiab_powerbi`.
# MAGIC
# MAGIC Déroulé :
# MAGIC 1. Setup (Spark) + **widgets de run** (année d'inventaire 2023 / 2024)
# MAGIC 2. Construction de `df_result` (`main.build_df_result`)
# MAGIC 3. Synthèse console (rappel)
# MAGIC 4. **Métriques** : une fonction par indicateur, affichées en table
# MAGIC    (dont chute par ancienneté, distribution des écarts de PM et
# MAGIC    investigation des orphelins)
# MAGIC 5. **Export** des métriques — tables Delta (référence) + fichiers DBFS
# MAGIC 6. Graphiques de restitution *(optionnel)*
# MAGIC
# MAGIC L'**année d'inventaire** se choisit via le widget en tête (2023 / 2024) ;
# MAGIC les fichiers MRM, la vision CPT et la date sont des widgets éditables.
# MAGIC Pour dérouler UNE vision avec toutes les explications : 📗
# MAGIC `itip_fiab_vision_cc2023` / 📘 `itip_fiab_vision_cc2024` (sans écriture).
# MAGIC Pour comparer les deux années côte à côte : `itip_fiab_comparaison`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup — session Spark + widgets de run
# MAGIC
# MAGIC Le notebook vit dans un dossier Repos Databricks : la racine du repo est
# MAGIC **automatiquement** sur `sys.path`, aucun chemin à ajouter.

# COMMAND ----------

from config import ANNEE_INVENTAIRE, INVENTAIRES
from core.runtime import configurer_run, get_spark
from core.synthese.kpi_export import print_synthese
from core import metrics
from main import build_df_result

spark = get_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets — année d'inventaire + sources
# MAGIC
# MAGIC `annee_inventaire` sélectionne le run (2023 / 2024). Les valeurs 2023 sont
# MAGIC pré-remplies (cf. `profile.py`) ; **renseigner les chemins MRM 2024** (et la
# MAGIC vision CC2024) avant de lancer le run 2024.

# COMMAND ----------

dbutils.widgets.dropdown("annee_inventaire", ANNEE_INVENTAIRE, list(INVENTAIRES), "Année d'inventaire")

# Défauts par année : config/profile.py (INVENTAIRES) — source unique des
# chemins. Les widgets restent éditables au run (ex. MRM 2024 à ajuster).
for _annee, _inv in INVENTAIRES.items():
    dbutils.widgets.text(f"date_{_annee}",   _inv["date"],   f"{_annee} · date d'inventaire")
    dbutils.widgets.text(f"vision_{_annee}", _inv["vision"], f"{_annee} · vision CPT")
    dbutils.widgets.text(f"mrm_{_annee}",    _inv["mrm"],    f"{_annee} · MRM courant")
    dbutils.widgets.text(f"mrm_{_annee}_n1", _inv["mrm_n1"], f"{_annee} · MRM N+1 (option)")

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
# MAGIC repêchage statut NON → obs tardives IT → tags. Le cœur métier de
# MAGIC `main.run`, **sans la restitution ni l'export** (pilotés ici cellule par
# MAGIC cellule) : cette cellule n'écrit rien.
# MAGIC
# MAGIC Tout le MRM traverse toutes les étapes de matching, « à supprimer »
# MAGIC compris : `MRM_DELETE` est un **état terminal** (retrouvé par aucune clé ⇒
# MAGIC suppression effective), au même niveau que les orphelins. Un « à
# MAGIC supprimer » retrouvé reste un `MATCH_*` — c'est un « encore au compte ».

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
# MAGIC ## 4. Métriques — les 9 tables (une table = un sujet complet)
# MAGIC
# MAGIC Des fonctions simples (`core.metrics`) : les tables scalaires prennent `d`
# MAGIC (`dim_run`, `synthese`, `bilan_cas`, `consignes`, `couverture`) ; `chute`
# MAGIC et `orphelins` ré-agrègent `df_result` côté Spark. Les angles d'analyse
# MAGIC sont des **colonnes** (`EXERCICE`, `AXE`, `SEGMENT`, `UNIVERS`) — mêmes
# MAGIC tables que l'export Power BI, reliées entre elles par `CLE_RUN` à l'export
# MAGIC (modèle en étoile, pivot `dim_run`).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Synthèse (1 ligne) + bilan cas par cas

# COMMAND ----------

display(metrics.synthese(d))

# COMMAND ----------

display(metrics.bilan_cas(d))   # LE bilan cas par cas (avec explications)

# COMMAND ----------

# MAGIC %md
# MAGIC ### La chute sous tous ses angles — `AXE` × `EXERCICE`
# MAGIC
# MAGIC `AXE` = Ensemble (le taux officiel) / Type de compte / Ancienneté /
# MAGIC Tranche d'écart ; `EXERCICE` sépare l'inventaire courant (stats globales)
# MAGIC des récupérés N+1 (analyse séparée). Dans chaque bloc, Σ des lignes
# MAGIC redonne le taux « Ensemble » ; l'ancienneté suit la méthode d'inventaire
# MAGIC (revue tête par tête sur N-1) ; `ORDRE` trie les segments dans chaque axe.

# COMMAND ----------

display(metrics.chute(df_result, d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### La distribution des écarts de PM — dossiers sur/sous-provisionnés
# MAGIC
# MAGIC Le taux agrégé donne le solde ; la distribution donne le détail : combien
# MAGIC de dossiers portent un écart, dans quel sens (positif = sous-provisionné,
# MAGIC risque ; négatif = sur-provisionné, marge) et à quelle ampleur — un taux
# MAGIC quasi nul peut cacher de gros écarts compensés. Tranches par seuils
# MAGIC (`SEUILS_ECART_PM`, config) : ±1 k€, ±5 k€, ±20 k€, ±100 k€ ; les vides
# MAGIC restent à zéro (axe stable).

# COMMAND ----------

display(metrics.chute_par_tranche_ecart(df_result))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Le suivi des consignes — les deux exercices (`EXERCICE`)
# MAGIC
# MAGIC Conformité + PM + chute par consigne (exercice courant pur, ligne « Sans
# MAGIC consigne reconnue » incluse) ; bloc « Récupérés N+1 » = analyse séparée.
# MAGIC Puis le tableau de bord ventilé par type de compte.

# COMMAND ----------

display(metrics.consignes(d))

# COMMAND ----------

display(metrics.consignes_par_type_compte(df_result))

# COMMAND ----------

# MAGIC %md
# MAGIC ### La couverture — les deux univers (`UNIVERS`)
# MAGIC
# MAGIC « Compte » : qui justifie chaque ligne du compte (retrouvés, N+1,
# MAGIC repêchés, clos, anomalies) ; « Revue MRM » : part de la revue retrouvée +
# MAGIC non retrouvés par consigne.

# COMMAND ----------

display(metrics.couverture(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### L'investigation des orphelins — six angles (`AXE`)
# MAGIC
# MAGIC Type de compte / garantie / ancienneté / mois de survenance partitionnent
# MAGIC les orphelins (Σ = total, chacun). « Clause (détail) » : sous-ensemble des
# MAGIC porteurs de clause, `ORDRE` 1 = le plus représentatif (à investiguer avec
# MAGIC le souscripteur). « Composante de clé nulle » : fréquence de nullité de
# MAGIC chaque composante (explique pourquoi ces dossiers n'ont pas matché).

# COMMAND ----------

display(metrics.orphelins(df_result, annee_inv))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Export des métriques
# MAGIC
# MAGIC Sorties pilotées par `config/profile.py` (`EXPORT_FORMATS`,
# MAGIC `EXPORT_DELTA_SCHEMA`) — les mêmes que le Job : tables Delta du metastore
# MAGIC en **référence**, fichiers DBFS (`.../<PERIMETRE>/metrics`) en secondaire.
# MAGIC
# MAGIC ⚠ **Cette cellule écrit dans le metastore.** L'historisation se fait par
# MAGIC `DATE_INVENTAIRE × PERIMETRE` : rejouer une année remplace exactement ses
# MAGIC lignes (2023 et 2024 coexistent), mais un run avec des **widgets modifiés**
# MAGIC (autre fichier MRM) sous une date officielle **écrase la partition
# MAGIC officielle**. Pour explorer sans rien écrire, sauter cette cellule, ou
# MAGIC forcer une cible de test : `delta_schema="hive_metastore.itip_backtest_test"`.

# COMMAND ----------

from config import EXPORT_DELTA_SCHEMA, EXPORT_FORMATS

_ = metrics.export_metriques(
    df_result, d,
    formats      = EXPORT_FORMATS,
    delta_schema = EXPORT_DELTA_SCHEMA,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Graphiques de restitution *(optionnel)*
# MAGIC
# MAGIC Restitution matplotlib (affichage + PNG DBFS), 12 graphiques (dont chute
# MAGIC par ancienneté, distribution des écarts et orphelins par compte).
# MAGIC Décommentez pour lancer.

# COMMAND ----------

# from core.metrics.viz import restituer_graphiques
# figs = restituer_graphiques(df_result, d)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC `df_result` reste en cache pour vos explorations ad hoc. Pour libérer : `df_result.unpersist()`.
