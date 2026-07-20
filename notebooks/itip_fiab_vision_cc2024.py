# Databricks notebook source
# MAGIC %md
# MAGIC # 📘 ITIP-FIAB — Vision CC2024 (inventaire du 31/12/2024)
# MAGIC
# MAGIC **Le notebook de recette de la vision comptable CC2024** : il déroule tout
# MAGIC le backtest de l'exercice 2024 — pipeline, synthèse, les 9 tables
# MAGIC métriques, la distribution des écarts de PM et les 12 graphiques — **sans
# MAGIC rien écrire** (ni table Delta, ni fichier, ni PNG). L'export officiel de
# MAGIC cette vision se fait par le Job 🚀 `itip_fiab_powerbi` (widget
# MAGIC `annee_inventaire = 2024`).
# MAGIC
# MAGIC ### Ce qui caractérise la vision CC2024
# MAGIC | Élément | Valeur | Conséquence |
# MAGIC |---|---|---|
# MAGIC | Vision comptable CPT | `CC2024` | le compte est filtré sur cette vision |
# MAGIC | Date d'inventaire | `31/12/2024` | clé d'historisation + découpage d'ancienneté (N = 2024) |
# MAGIC | Revue MRM courante | `MRM_Fiab_31_12_24` (**à déposer sur DBFS**) | l'estimation auditée |
# MAGIC | Revue MRM N+1 | **aucune pour l'instant** (inventaire du 30/06/2025 non disponible) | pas de récupération tardive : `CPT_LATE` vide, taux de récupération **« non mesuré »** (≠ 0 % de performance), blocs « Récupérés N+1 » vides dans `chute` / `consignes` |
# MAGIC
# MAGIC > 💡 Dès que l'inventaire du 30/06/2025 sera déposé, renseigner son chemin
# MAGIC > dans le widget `fichier_mrm_n1` (et dans `INVENTAIRES["2024"]["mrm_n1"]`
# MAGIC > de `config/profile.py`) puis rejouer : la récupération tardive s'activera
# MAGIC > sans autre changement.
# MAGIC
# MAGIC > 📚 Contrats : [`docs/METRIQUES.md`](../docs/METRIQUES.md) (formules et
# MAGIC > univers) · [`docs/RECETTE_ETUDE.md`](../docs/RECETTE_ETUDE.md)
# MAGIC > (fabrication de bout en bout) ·
# MAGIC > [`docs/POWERBI_MAQUETTE.md`](../docs/POWERBI_MAQUETTE.md) (restitution).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ⚙️ Setup — session Spark + paramètres de la vision
# MAGIC
# MAGIC Les widgets sont **pré-remplis avec la configuration officielle 2024**
# MAGIC (`config/profile.py`, dictionnaire `INVENTAIRES["2024"]`) et restent
# MAGIC éditables — **vérifier que le fichier MRM 2024 est bien déposé** au chemin
# MAGIC indiqué avant de lancer. Le notebook vit dans un dossier Repos
# MAGIC Databricks : la racine du dépôt de code est déjà sur `sys.path`.

# COMMAND ----------

from config import INVENTAIRES
from core.runtime import configurer_run, get_spark
from core.synthese.kpi_export import print_synthese
from core import metrics
from main import build_df_result

spark  = get_spark()
ANNEE  = "2024"
_inv   = INVENTAIRES[ANNEE]

dbutils.widgets.text("date_inventaire", _inv["date"],   "Date d'inventaire")
dbutils.widgets.text("vision_cpt",      _inv["vision"], "Vision CPT")
dbutils.widgets.text("fichier_mrm",     _inv["mrm"],    "MRM courant")
dbutils.widgets.text("fichier_mrm_n1",  _inv["mrm_n1"], "MRM N+1 (vide = sans)")

profil = configurer_run(
    date_inventaire = dbutils.widgets.get("date_inventaire"),
    cpt_vision      = dbutils.widgets.get("vision_cpt"),
    fichier_mrm     = dbutils.widgets.get("fichier_mrm"),
    fichier_mrm_n1  = dbutils.widgets.get("fichier_mrm_n1") or None,
)

displayHTML(f"""
<div style="font-family:'Segoe UI',sans-serif;border:1px solid #d5d9e0;border-radius:10px;
            padding:14px 18px;background:linear-gradient(90deg,#f4f6fb,#ffffff);max-width:860px">
  <div style="font-size:15px;font-weight:600;color:#00008F">📘 Run de recette — vision CC2024</div>
  <table style="margin-top:8px;font-size:13px;color:#333;border-collapse:collapse">
    <tr><td style="padding:2px 14px 2px 0;color:#666">Vision CPT</td><td><b>{profil["cpt_vision"]}</b></td></tr>
    <tr><td style="padding:2px 14px 2px 0;color:#666">Date d'inventaire</td><td><b>{profil["date_inventaire"]}</b></td></tr>
    <tr><td style="padding:2px 14px 2px 0;color:#666">Revue MRM</td><td>{profil["fichier_mrm"]}</td></tr>
    <tr><td style="padding:2px 14px 2px 0;color:#666">Revue MRM N+1</td><td>{profil["fichier_mrm_n1"] or "— (pas de récupération tardive : blocs N+1 vides)"}</td></tr>
  </table>
  <div style="margin-top:8px;font-size:12px;color:#027180">✅ Notebook sans écriture — l'export officiel passe par le Job itip_fiab_powerbi.</div>
</div>""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 🏗️ Pipeline — construction de `df_result`
# MAGIC
# MAGIC `main.build_df_result` (le cœur métier, **aucune écriture**) enchaîne :
# MAGIC chargement (compte filtré sur la vision CC2024 + revue MRM) → nettoyage et
# MAGIC clés → **waterfall de matching** (14 étapes, du plus strict au plus
# MAGIC flexible) → repêchage **statut NON** (hors métriques) → observations
# MAGIC tardives IT → tags persistants.
# MAGIC
# MAGIC Sans MRM N+1, l'étape de récupération tardive est simplement **absente** :
# MAGIC un orphelin compte reste `CPT_ONLY` (ou bascule en observation tardive IT
# MAGIC s'il en remplit les critères). À l'arrivée : **une ligne par dossier, une
# MAGIC catégorie par ligne** (`TYPE_RECONCILIATION`).

# COMMAND ----------

df_result = build_df_result(spark).persist()
print(f"🏗️  df_result CC2024 : {df_result.count():,} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 🔎 Synthèse console + contrôles de cohérence
# MAGIC
# MAGIC La vue d'ensemble ASCII dans le **vocabulaire client à deux couches** :
# MAGIC *retrouvé / non retrouvé* (le fait), *conforme / encore au compte* (le
# MAGIC verdict). Sur cette vision, lire le taux de récupération tardive comme
# MAGIC **« non mesuré »** (pas de N+1), et s'attendre à des anomalies plus
# MAGIC nombreuses qu'en 2023 : personne n'est encore venu les repêcher.
# MAGIC
# MAGIC `print_synthese` renvoie `d` : la passe Spark est faite **une seule
# MAGIC fois**, réutilisée par toutes les tables. Les deux contrôles ci-dessous
# MAGIC sont ceux qui **bloquent** le run de production.

# COMMAND ----------

d = print_synthese(df_result)
annee_inv = metrics._annee_inventaire(d)

assert d["coherent"],        f"❌ Lignes non classées : {d['labels_inconnus']}"
assert d["chute_coherente"], "❌ Taux de chute ≠ Σ consignes + hors consigne (voir logs)"
print("✅ Contrôles de synthèse OK — lignes toutes classées, chute cohérente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 📦 Les 9 tables métriques — mêmes tables que l'export Power BI
# MAGIC
# MAGIC Une table = **un sujet complet** ; les angles d'analyse sont des
# MAGIC **colonnes** (`EXERCICE`, `AXE`, `SEGMENT`, `UNIVERS`), jamais des tables
# MAGIC séparées : la restitution **filtre**, elle n'assemble pas. Toutes portent
# MAGIC à l'export la clé de liaison `CLE_RUN` (date × périmètre) qui les relie à
# MAGIC la dimension `dim_run` — le modèle en étoile du rapport.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Synthèse — tous les KPI du run en une ligne
# MAGIC
# MAGIC La « carte d'identité » de l'exercice 2024. Historisée par run : une fois
# MAGIC exportée par le Job, elle coexiste avec la ligne 2023 — c'est la source
# MAGIC des courbes d'évolution du rapport (2023 → 2024).

# COMMAND ----------

display(metrics.synthese(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Bilan cas par cas — LE tableau de restitution
# MAGIC
# MAGIC Chaque ligne = un cas avec sa volumétrie, ses PM et sa **phrase
# MAGIC d'explication**. Sur cette vision, les lignes « Récupérés dans le MRM
# MAGIC N+1 » et « Repêchés statut NON — exercice N+1 » sont à zéro : l'inventaire
# MAGIC suivant n'existe pas encore.

# COMMAND ----------

display(metrics.bilan_cas(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Couverture — les deux univers (`UNIVERS`)
# MAGIC
# MAGIC - **« Compte »** : le compte est-il justifié ? — sans N+1, la
# MAGIC   justification repose sur les seuls retrouvés de l'inventaire courant
# MAGIC   (+ repêchés statut NON, + clos explicables).
# MAGIC - **« Revue MRM »** : la revue est-elle retrouvée ? Part retrouvée +
# MAGIC   non retrouvés par consigne (+ « à supprimer » encore au compte).

# COMMAND ----------

display(metrics.couverture(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Chute — le taux sous tous ses angles (`AXE` × `EXERCICE`)
# MAGIC
# MAGIC `AXE` = **Ensemble** / **Type de compte** / **Ancienneté** (N = 2024,
# MAGIC N-1 = 2023, N-2 et antérieur — la revue tête par tête porte sur N-1) /
# MAGIC **Tranche d'écart** (distribution des écarts, §5). Le bloc `EXERCICE` =
# MAGIC « Récupérés N+1 » est **vide sur cette vision** (pas d'inventaire
# MAGIC suivant) : seul l'inventaire courant alimente les stats. `ORDRE` trie les
# MAGIC segments dans chaque axe (« Trier par colonne » dans Power BI).

# COMMAND ----------

display(metrics.chute(df_result, d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Consignes — conformité, PM et chute par consigne (`EXERCICE`)
# MAGIC
# MAGIC Bloc **« Inventaire courant »** : conformité (KO nommé par le fait — *non
# MAGIC retrouvé* / *encore au compte*), base PM et taux de chute par consigne,
# MAGIC plus la ligne « Sans consigne reconnue ». Pas de bloc N+1 sur cette
# MAGIC vision. Puis le **tableau de bord par type de compte**.

# COMMAND ----------

display(metrics.consignes(d))

# COMMAND ----------

display(metrics.consignes_par_type_compte(df_result))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Orphelins — l'investigation sous six angles (`AXE`)
# MAGIC
# MAGIC Les dossiers du compte **sans contrepartie MRM**. Attention à la lecture
# MAGIC 2024 : sans N+1, cette population contient encore les futures
# MAGIC déclarations tardives — elle se **résorbera mécaniquement** quand
# MAGIC l'inventaire du 30/06/2025 sera intégré. Les axes : type de compte,
# MAGIC garantie, ancienneté, mois de survenance (partitionnants), clause
# MAGIC (détail, `ORDRE` 1 = compte à investiguer en premier) et composante de
# MAGIC clé nulle (pourquoi ça n'a pas matché).

# COMMAND ----------

display(metrics.orphelins(df_result, annee_inv))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.7 Contrôles de cohérence + dimension de run
# MAGIC
# MAGIC `controles_coherence` : les recoupements inter-tables (attendu / obtenu /
# MAGIC OK). `dim_run` : la dimension du run — noter `AVEC_MRM_N1 = false`, c'est
# MAGIC elle qui explique au rapport pourquoi les blocs N+1 de cette vision sont
# MAGIC vides.

# COMMAND ----------

tables = metrics.toutes_metriques(df_result, d)
ctrl   = tables["controles_coherence"]
display(ctrl)
assert ctrl["OK"].all(), f"❌ {int((~ctrl['OK']).sum())} recoupement(s) KO"
print(f"✅ Recoupements inter-tables : {len(ctrl)}/{len(ctrl)} OK.")
display(tables["dim_run"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 📊 Distribution des écarts de PM — dossiers sur/sous-provisionnés
# MAGIC
# MAGIC Le taux de chute agrégé donne **le solde** ; la distribution donne **le
# MAGIC détail dossier par dossier** : combien portent un écart, dans quel sens et
# MAGIC à quelle ampleur — un taux quasi nul peut cacher de gros écarts compensés.
# MAGIC
# MAGIC L'écart signé (`PM_MRM − PM_CPT`) de chaque dossier de l'univers de chute
# MAGIC est classé dans une **tranche de seuils** (config `SEUILS_ECART_PM` :
# MAGIC ±1 k€, ±5 k€, ±20 k€, ±100 k€) : écart **positif = sous-provisionné**
# MAGIC (risque), **négatif = sur-provisionné** (marge). Les tranches vides
# MAGIC restent affichées à zéro (axe stable). C'est l'axe « Tranche d'écart » de
# MAGIC la table `chute` — modifier les seuils ne demande qu'une ligne de config.

# COMMAND ----------

tranches = metrics.chute_par_tranche_ecart(df_result)
display(tranches[tranches["EXERCICE"] == metrics.EXERCICE_INV])

# COMMAND ----------

# Volumétrie totale par sens de l'écart (inventaire courant).
_inv_tr = tranches[tranches["EXERCICE"] == metrics.EXERCICE_INV]
print(f"Sous-provisionnés (écart > 0, risque) : {int(_inv_tr['NB_SOUS_PROVISION'].sum()):,} dossiers")
print(f"Sur-provisionnés  (écart < 0, marge)  : {int(_inv_tr['NB_SUR_PROVISION'].sum()):,} dossiers")
print(f"À l'équilibre     (écart = 0)         : {int(_inv_tr['NB_ECART_NUL'].sum()):,} dossiers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 🎨 Les 12 graphiques de restitution *(affichés, non écrits)*
# MAGIC
# MAGIC Chaque titre porte la **conclusion** (pas le sujet). `save_dir=None` :
# MAGIC aucun PNG déposé.

# COMMAND ----------

from core.metrics.viz import restituer_graphiques

figs = restituer_graphiques(df_result, d, save_dir=None)
print(f"✔ {len(figs)} graphiques rendus")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. ✅ Récapitulatif de la vision CC2024

# COMMAND ----------

displayHTML(f"""
<div style="font-family:'Segoe UI',sans-serif;border:1px solid #d5d9e0;border-radius:10px;
            padding:14px 18px;background:#f7f9fc;max-width:860px">
  <div style="font-size:15px;font-weight:600;color:#00008F">✅ Recette CC2024 terminée — aucune écriture</div>
  <ul style="margin:8px 0 4px 18px;padding:0;font-size:13px;color:#333;line-height:1.7">
    <li><b>{df_result.count():,}</b> dossiers réconciliés — lignes toutes classées, chute cohérente ;</li>
    <li>taux de chute inventaire : <b>{d["taux_chute_inventaire"]} %</b> — pas de N+1 sur cette vision
        (récupération tardive « non mesurée », à rejouer avec l'inventaire du 30/06/2025) ;</li>
    <li>9 tables calculées et recoupées ({len(ctrl)}/{len(ctrl)} contrôles OK), 12 graphiques rendus ;</li>
    <li>export officiel : Job <b>itip_fiab_powerbi</b>, widget <code>annee_inventaire = 2024</code>
        → tables Delta <code>hive_metastore.itip_backtest.*</code> historisées 31/12/2024 × MULTI —
        2023 et 2024 coexistent, le rapport les compare nativement.</li>
  </ul>
</div>""")
df_result.unpersist()
