# Databricks notebook source
# MAGIC %md
# MAGIC # 📗 ITIP-FIAB — Vision CC2023 (inventaire du 31/12/2023)
# MAGIC
# MAGIC **Le notebook de recette de la vision comptable CC2023** : il déroule tout
# MAGIC le backtest de l'exercice 2023 — pipeline, synthèse, les 10 tables
# MAGIC métriques, la distribution des écarts de PM et les 12 graphiques — **sans
# MAGIC rien écrire** (ni table Delta, ni fichier, ni PNG). L'export officiel de
# MAGIC cette vision se fait par le Job 🚀 `itip_fiab_powerbi` (widget
# MAGIC `annee_inventaire = 2023`).
# MAGIC
# MAGIC ### Ce qui caractérise la vision CC2023
# MAGIC | Élément | Valeur | Conséquence |
# MAGIC |---|---|---|
# MAGIC | Vision comptable CPT | `CC2023` | le compte est filtré sur cette vision |
# MAGIC | Date d'inventaire | `31/12/2023` | clé d'historisation + découpage d'ancienneté (N = 2023) |
# MAGIC | Revue MRM courante | `MRM_Fiab_31_12_23_V3` | l'estimation auditée |
# MAGIC | Revue MRM N+1 | `MRM_Fiab_30_06_24` (30/06/2024) | **récupération des déclarations tardives ACTIVE** : les orphelins compte sont rejoués contre l'inventaire suivant (`CPT_LATE`, analyse séparée) |
# MAGIC
# MAGIC > 📚 Contrats : [`docs/METRIQUES.md`](../docs/METRIQUES.md) (formules et
# MAGIC > univers) · [`docs/RECETTE_ETUDE.md`](../docs/RECETTE_ETUDE.md)
# MAGIC > (fabrication de bout en bout) ·
# MAGIC > [`docs/POWERBI_MAQUETTE.md`](../docs/POWERBI_MAQUETTE.md) (restitution).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ⚙️ Setup — session Spark + paramètres de la vision
# MAGIC
# MAGIC Les widgets sont **pré-remplis avec la configuration officielle 2023**
# MAGIC (`config/profile.py`, dictionnaire `INVENTAIRES["2023"]`) et restent
# MAGIC éditables (ex. tester un autre dépôt du fichier MRM). Le notebook vit dans
# MAGIC un dossier Repos Databricks : la racine du dépôt de code est déjà sur
# MAGIC `sys.path`.

# COMMAND ----------

from config import INVENTAIRES
from core.runtime import configurer_run, get_spark
from core.synthese.kpi_export import print_synthese
from core import metrics
from main import build_df_result

spark  = get_spark()
ANNEE  = "2023"
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
  <div style="font-size:15px;font-weight:600;color:#00008F">📗 Run de recette — vision CC2023</div>
  <table style="margin-top:8px;font-size:13px;color:#333;border-collapse:collapse">
    <tr><td style="padding:2px 14px 2px 0;color:#666">Vision CPT</td><td><b>{profil["cpt_vision"]}</b></td></tr>
    <tr><td style="padding:2px 14px 2px 0;color:#666">Date d'inventaire</td><td><b>{profil["date_inventaire"]}</b></td></tr>
    <tr><td style="padding:2px 14px 2px 0;color:#666">Revue MRM</td><td>{profil["fichier_mrm"]}</td></tr>
    <tr><td style="padding:2px 14px 2px 0;color:#666">Revue MRM N+1</td><td>{profil["fichier_mrm_n1"] or "— (pas de récupération tardive)"}</td></tr>
  </table>
  <div style="margin-top:8px;font-size:12px;color:#027180">✅ Notebook sans écriture — l'export officiel passe par le Job itip_fiab_powerbi.</div>
</div>""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 🏗️ Pipeline — construction de `df_result`
# MAGIC
# MAGIC `main.build_df_result` (le cœur métier, **aucune écriture**) enchaîne :
# MAGIC chargement (compte filtré sur la vision CC2023 + revue MRM) → nettoyage et
# MAGIC clés → **waterfall de matching** (14 étapes, du plus strict au plus
# MAGIC flexible) → récupération **N+1** (orphelins rejoués contre l'inventaire du
# MAGIC 30/06/2024) → repêchage **statut NON** (hors métriques) → observations
# MAGIC tardives IT → tags persistants.
# MAGIC
# MAGIC À l'arrivée : **une ligne par dossier, une catégorie par ligne**
# MAGIC (`TYPE_RECONCILIATION`) — c'est ce qui garantit qu'aucun dossier n'est
# MAGIC compté deux fois dans les métriques.

# COMMAND ----------

df_result = build_df_result(spark).persist()
print(f"🏗️  df_result CC2023 : {df_result.count():,} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 🔎 Synthèse console + contrôles de cohérence
# MAGIC
# MAGIC La vue d'ensemble ASCII (bulles REVUE / RETROUVÉS / COMPTE, taux, niveaux
# MAGIC de PM, blocs séparés N+1 et statut NON) dans le **vocabulaire client à
# MAGIC deux couches** : *retrouvé / non retrouvé* (le fait), *conforme / encore
# MAGIC au compte* (le verdict).
# MAGIC
# MAGIC `print_synthese` renvoie `d` : la passe Spark est faite **une seule
# MAGIC fois** et réutilisée par toutes les tables — aucune grandeur ne peut
# MAGIC diverger entre deux affichages. Les deux contrôles ci-dessous sont ceux
# MAGIC qui **bloquent** le run de production.

# COMMAND ----------

d = print_synthese(df_result)
annee_inv = metrics._annee_inventaire(d)

assert d["coherent"],        f"❌ Lignes non classées : {d['labels_inconnus']}"
assert d["chute_coherente"], "❌ Taux de chute ≠ Σ consignes + hors consigne (voir logs)"
print("✅ Contrôles de synthèse OK — lignes toutes classées, chute cohérente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 📦 Les 10 tables métriques — mêmes tables que l'export Power BI
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
# MAGIC L'équivalent « carte d'identité » de l'exercice 2023 : taux de chute
# MAGIC (inventaire + N+1 séparé), conformité globale, couvertures, niveaux de PM
# MAGIC (retrouvés **vs** base chute — deux univers à ne pas mélanger),
# MAGIC volumétries. Historisée par run → c'est elle qui alimente les courbes
# MAGIC 2023 / 2024 du rapport.

# COMMAND ----------

display(metrics.synthese(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Bilan cas par cas — LE tableau de restitution
# MAGIC
# MAGIC Chaque ligne = un cas de la réconciliation avec sa volumétrie, ses PM et
# MAGIC sa **phrase d'explication** : matchés par famille de clé (principale /
# MAGIC affinée / récupération / clé clause), base du taux de chute, récupérés
# MAGIC N+1, repêchés statut NON (ventilés N / N+1), non retrouvés des deux côtés,
# MAGIC « à supprimer » encore au compte. À lire de haut en bas — c'est le
# MAGIC déroulé de l'histoire.

# COMMAND ----------

display(metrics.bilan_cas(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Couverture — les deux univers (`UNIVERS`)
# MAGIC
# MAGIC - **« Compte »** : le compte est-il justifié ? Décomposition en retrouvés,
# MAGIC   récupérés N+1, repêchés, clos avant inventaire, anomalies — poids en
# MAGIC   nombre **et** en PM.
# MAGIC - **« Revue MRM »** : la revue est-elle retrouvée ? Part retrouvée au
# MAGIC   compte + non retrouvés par consigne (+ « à supprimer » encore au compte).

# COMMAND ----------

display(metrics.couverture(d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Chute — le taux sous tous ses angles (`AXE` × `EXERCICE`)
# MAGIC
# MAGIC `AXE` = **Ensemble** (le taux officiel : composantes PM exactes du ratio) /
# MAGIC **Type de compte** (PB / HPB / …) / **Ancienneté** (N / N-1 / N-2 et
# MAGIC antérieur — la méthode d'inventaire diffère selon l'année, revue tête par
# MAGIC tête sur N-1) / **Tranche d'écart** (la distribution des écarts, détaillée
# MAGIC en §5). `EXERCICE` sépare l'inventaire courant (**les stats globales**)
# MAGIC des récupérés N+1 (**analyse séparée**). Dans chaque bloc, Σ des lignes
# MAGIC redonne le taux « Ensemble » — garanti par les recoupements. `ORDRE`
# MAGIC trie les segments dans chaque axe (« Trier par colonne » dans Power BI).

# COMMAND ----------

display(metrics.chute(df_result, d))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Consignes — conformité, PM et chute par consigne (`EXERCICE`)
# MAGIC
# MAGIC Bloc **« Inventaire courant »** : conformité (le KO est nommé par le fait —
# MAGIC *non retrouvé* pour conserver/ajouter/étudier, *encore au compte* pour à
# MAGIC supprimer), la base PM et le taux de chute par consigne, plus la ligne
# MAGIC « Sans consigne reconnue » (matchés sans consigne exploitable : pas de
# MAGIC conformité, mais leur PM est dans la base chute). Bloc **« Récupérés
# MAGIC N+1 »** : le suivi de l'inventaire suivant, à part. Puis le **tableau de
# MAGIC bord par type de compte** (la même règle, ventilée).

# COMMAND ----------

display(metrics.consignes(d))

# COMMAND ----------

display(metrics.consignes_par_type_compte(df_result))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Orphelins — l'investigation sous six angles (`AXE`)
# MAGIC
# MAGIC Les dossiers du compte **sans contrepartie MRM** (anomalies définitives).
# MAGIC Quatre axes **partitionnent** (Σ = total, chacun) : type de compte,
# MAGIC garantie (IT 60 / IP 64), ancienneté, mois de survenance (effet fin
# MAGIC d'année). Deux axes de détail : **« Clause (détail) »** (`ORDRE` 1 = le
# MAGIC compte le plus représentatif, à investiguer avec le souscripteur) et
# MAGIC **« Composante de clé nulle »** (pourquoi ça n'a pas matché : RPP,
# MAGIC clause… — non additif).

# COMMAND ----------

display(metrics.orphelins(df_result, annee_inv))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.7 🎯 Clauses PB à investiguer — anomalies du compte
# MAGIC
# MAGIC Préparation des échanges avec **les préparateurs de comptes** : parmi les
# MAGIC anomalies du compte (orphelins définitifs, sans contrepartie MRM), on
# MAGIC isole les clauses **PB** les plus représentatives — les **2 premières en
# MAGIC volumétrie de dossiers** et les **2 premières en poids de PM** (2 à
# MAGIC 4 clauses selon recouvrement). Ce sont les comptes sur lesquels porter
# MAGIC l'investigation au cas par cas, conformément au circuit de remontée
# MAGIC (chaque anomalie revient à la personne qui a réalisé le compte).
# MAGIC
# MAGIC Les poids se lisent **en part du total des anomalies** (tous types de
# MAGIC compte, porteuses de clause ou non) — cf. `orphelins_par_clause`.
# MAGIC
# MAGIC > ⚠ L'identité du **préparateur du compte** n'existe pas dans les données
# MAGIC > chargées ici. Pour la retrouver (et tout le contexte de saisie), passer
# MAGIC > par le notebook 🔍 `itip_fiab_exploration_clauses` : requêtes directes
# MAGIC > sur les tables brutes du schéma `compteclient`, clause en widget.
# MAGIC >
# MAGIC > 📎 Pour préparer l'échange lui-même, le notebook
# MAGIC > `itip_fiab_extraction_anomalies` produit **un classeur par clause** avec
# MAGIC > les lignes tête par tête concernées (la pièce jointe du message).

# COMMAND ----------

import pandas as pd

_clauses_orph = metrics.orphelins_par_clause(df_result)
_pb_orph      = _clauses_orph[_clauses_orph["TYPE_COMPTE"] == "PB"].reset_index(drop=True)

_top_nb = _pb_orph.nlargest(2, "NB_DOSSIERS").assign(CRITERE="Volumétrie dossiers")
_top_pm = _pb_orph.nlargest(2, "PM_CPT").assign(CRITERE="Poids de PM")

cibles_pb = (
    pd.concat([_top_nb, _top_pm], ignore_index=True)
      .groupby(["CLAUSE", "TYPE_COMPTE", "NB_DOSSIERS", "PM_CPT",
                "POIDS_NB_PCT", "POIDS_PM_PCT"], as_index=False)["CRITERE"]
      .agg(" + ".join)
      .sort_values(["NB_DOSSIERS", "PM_CPT"], ascending=False)
      .reset_index(drop=True)
)

print(f"🎯 {len(cibles_pb)} clause(s) PB cible(s) pour l'investigation :")
for _, r in cibles_pb.iterrows():
    print(f"   · Clause {r['CLAUSE']} — {int(r['NB_DOSSIERS']):,} dossiers"
          f" ({r['POIDS_NB_PCT']} % des anomalies) · PM {r['PM_CPT']:,.0f} €"
          f" ({r['POIDS_PM_PCT']} % de la PM des anomalies)"
          f" · critère : {r['CRITERE']}")

display(cibles_pb[["CLAUSE", "TYPE_COMPTE", "CRITERE", "NB_DOSSIERS", "PM_CPT",
                   "POIDS_NB_PCT", "POIDS_PM_PCT"]])

# COMMAND ----------

# Contexte : les 10 premières clauses PB en anomalies (tri volumétrie).
display(_pb_orph.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.8 Contrôles de cohérence + dimension de run
# MAGIC
# MAGIC `controles_coherence` : les recoupements inter-tables (attendu / obtenu /
# MAGIC OK) — la table qui prouve que tous les onglets racontent **une seule
# MAGIC histoire**. `dim_run` : la dimension du run (année, vision, présence d'un
# MAGIC N+1), pivot du modèle en étoile côté Power BI.

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
# MAGIC Chaque titre porte la **conclusion** (pas le sujet) : justification du
# MAGIC compte, couverture de la revue, chute par type de compte / consigne /
# MAGIC ancienneté, conformité, anomalies, orphelins par compte, et la
# MAGIC **distribution des écarts** (graphe 12). `save_dir=None` : aucun PNG déposé.

# COMMAND ----------

from core.metrics.viz import restituer_graphiques

figs = restituer_graphiques(df_result, d, save_dir=None)
print(f"✔ {len(figs)} graphiques rendus")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. ✅ Récapitulatif de la vision CC2023

# COMMAND ----------

displayHTML(f"""
<div style="font-family:'Segoe UI',sans-serif;border:1px solid #d5d9e0;border-radius:10px;
            padding:14px 18px;background:#f7f9fc;max-width:860px">
  <div style="font-size:15px;font-weight:600;color:#00008F">✅ Recette CC2023 terminée — aucune écriture</div>
  <ul style="margin:8px 0 4px 18px;padding:0;font-size:13px;color:#333;line-height:1.7">
    <li><b>{df_result.count():,}</b> dossiers réconciliés — lignes toutes classées, chute cohérente ;</li>
    <li>taux de chute inventaire : <b>{d["taux_chute_inventaire"]} %</b> (N+1 séparé : {d["taux_chute_n1"]} %) ;</li>
    <li>10 tables calculées et recoupées ({len(ctrl)}/{len(ctrl)} contrôles OK), 12 graphiques rendus ;</li>
    <li>export officiel : Job <b>itip_fiab_powerbi</b>, widget <code>annee_inventaire = 2023</code>
        → tables Delta <code>hive_metastore.itip_backtest.*</code> historisées 31/12/2023 × MULTI.</li>
  </ul>
</div>""")
df_result.unpersist()
