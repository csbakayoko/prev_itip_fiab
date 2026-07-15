# Databricks notebook source
# MAGIC %md
# MAGIC # ITIP-FIAB — Run de production → Power BI
# MAGIC
# MAGIC Notebook **final** : données + config en entrée, tables métriques en sortie.
# MAGIC C'est **le seul notebook qui écrit** — les autres (smoke, comparaison,
# MAGIC key_audit) passent par `build_df_result` et ne touchent à rien.
# MAGIC
# MAGIC Déroulé :
# MAGIC 1. **Setup** — session Spark + paramètres du run (widgets = paramètres du Job,
# MAGIC    défauts `config/profile.py`)
# MAGIC 2. **Pipeline** — chargement → nettoyage → matching (11 étapes) → états
# MAGIC    terminaux (`MRM_DELETE`, orphelins) → récupérations (N+1, statut NON) → tags
# MAGIC 3. **Synthèse** — contrôles de cohérence console (`chute_coherente`, lignes classées)
# MAGIC 4. **Export** — les 22 tables métriques en Delta dans le metastore (la sortie
# MAGIC    de **référence**, celle que Power BI interroge) + fichiers DBFS en
# MAGIC    secondaire, puis contrôles inter-tables BLOQUANTS
# MAGIC 5. **Détail du run** — `df_result` écrit en table Delta `resultat_backtest`,
# MAGIC    historisée par `DATE_INVENTAIRE × PERIMETRE` (2023 et 2024 coexistent)
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
# MAGIC | `chute_par_type_compte` | taux de chute par TYPE_COMPTE (PB / HPB / …) × EXERCICE (inventaire courant / N+1 séparé) |
# MAGIC | `chute_par_anciennete` | taux de chute par année de survenance (N / N-1 / N-2 et antérieur) × EXERCICE |
# MAGIC | `consignes_par_type_compte` | tableau de bord : suivi des consignes par TYPE_COMPTE × CONSIGNE |
# MAGIC | `chute_par_consigne` / `pm_par_consigne` | chute et PM par consigne pertinente |
# MAGIC | `conformite_consignes` / `conformite_globale` | application des consignes (détail + segments) |
# MAGIC | `anomalies_cpt_only` | anomalies par mois de survenance (effet fin d'année) |
# MAGIC | `orphelins_par_type_compte` | orphelins compte par TYPE_COMPTE — ventilation complète |
# MAGIC | `orphelins_par_clause` | **détail** : orphelins des comptes porteurs d'une clause, RANG 1 = à investiguer |
# MAGIC | `orphelins_par_garantie` / `_par_anciennete` / `_cles_nulles` | investigation des orphelins |
# MAGIC | `controles_coherence` | recoupements inter-tables (attendu / obtenu / OK) — onglet « fiabilité » |
# MAGIC
# MAGIC > **Axe d'analyse** : les métriques se ventilent par `TYPE_COMPTE`, le
# MAGIC > périmètre métier. La clause n'est pas un axe (elle ne sert qu'à remplacer
# MAGIC > un RPP nul dans la clé de matching, et tous les comptes n'en portent pas) :
# MAGIC > elle ne subsiste que dans la table de détail `orphelins_par_clause`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup — session Spark + paramètres du run
# MAGIC
# MAGIC Le notebook vit dans un dossier Repos Databricks : la racine du repo est sur `sys.path`.
# MAGIC
# MAGIC **Tous les paramètres sont des widgets** (= « base parameters » du Job
# MAGIC Databricks) : le Job surcharge n'importe lequel sans toucher au code.
# MAGIC Un widget vide = défaut de `config/profile.py` (`INVENTAIRES[annee]`).

# COMMAND ----------

from config import (
    ANNEE_INVENTAIRE, CLIENT_NAME, CLIENT_TYPE_CLAUSES, EXPORT_DELTA_SCHEMA,
    EXPORT_FORMATS, INVENTAIRES,
)
from core.io.save_result import save_result_delta
from core.runtime import configurer_run, get_spark
from core.synthese.kpi_export import print_synthese
from core import metrics
from main import build_df_result

spark = get_spark()

# COMMAND ----------

# ── Paramètres du run (widgets = paramètres du Job) ──────────────────────────
dbutils.widgets.dropdown("annee_inventaire", ANNEE_INVENTAIRE, list(INVENTAIRES), "Année d'inventaire")
dbutils.widgets.text("date_inventaire", "", "Date d'inventaire (vide = config)")
dbutils.widgets.text("vision_cpt",      "", "Vision CPT (vide = config)")
dbutils.widgets.text("fichier_mrm",     "", "MRM courant (vide = config)")
dbutils.widgets.text("fichier_mrm_n1",  "", "MRM N+1 (vide = config, 'aucun' = sans)")
dbutils.widgets.text("types_compte",    ",".join(CLIENT_TYPE_CLAUSES or []) or "*",
                     "Types de compte (PB,HPB… ; * = tous)")
dbutils.widgets.text("delta_schema",    EXPORT_DELTA_SCHEMA or "", "Schéma Delta (vide = pas de Delta)")


def _param(nom: str, defaut: str) -> str:
    """Valeur du widget, ou le défaut de config si le widget est vide."""
    return dbutils.widgets.get(nom).strip() or defaut


_annee = dbutils.widgets.get("annee_inventaire")
_inv   = INVENTAIRES[_annee]

# 'aucun' force un run SANS récupération N+1 même si la config en définit une.
_mrm_n1 = _param("fichier_mrm_n1", _inv["mrm_n1"])
profil  = configurer_run(
    date_inventaire = _param("date_inventaire", _inv["date"]),
    cpt_vision      = _param("vision_cpt",      _inv["vision"]),
    fichier_mrm     = _param("fichier_mrm",     _inv["mrm"]),
    fichier_mrm_n1  = None if _mrm_n1.lower() in ("", "aucun") else _mrm_n1,
    types_compte    = dbutils.widgets.get("types_compte"),
)
print(f"Run inventaire {_annee} :", profil)

# ── Cible de l'export Power BI ───────────────────────────────────────────────
# RÉFÉRENCE = les tables Delta du metastore : Power BI se connecte au SQL
# Warehouse et lit <schema>.metrique_<nom> (noms stables ; le run est porté par
# les colonnes DATE_INVENTAIRE × PERIMETRE). Les fichiers DBFS (Excel/parquet/
# csv) sont la sortie SECONDAIRE : import fichier quand le Warehouse n'est pas
# disponible, dépannage, partage ponctuel.
# Les formats viennent de la config (EXPORT_FORMATS) — une seule source de
# vérité. Widget delta_schema vide = pas de Delta : on retire ce format et il ne
# reste que les fichiers.
DELTA_SCHEMA = _param("delta_schema", "") or None
FORMATS      = (
    tuple(EXPORT_FORMATS) if DELTA_SCHEMA
    else tuple(f for f in EXPORT_FORMATS if f.lower() != "delta")
)

print("Périmètre :", CLIENT_NAME)
print("Formats   :", FORMATS, "| schéma Delta :", DELTA_SCHEMA or "—")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline — construction de `df_result`
# MAGIC
# MAGIC `main.build_df_result` (le cœur de `main.run`, sans la restitution) :
# MAGIC matching principal (MRM statut OUI), récupération N+1, repêchage statut NON
# MAGIC (hors métriques), obs tardives IT, tags persistants. Sources et date pilotées
# MAGIC par les widgets ci-dessus (défauts : `config/profile.py`).

# COMMAND ----------

df_result = build_df_result(spark).persist()
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
# MAGIC Une passe : les 22 tables métriques écrites en Delta + parquet/csv sur DBFS.

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
# MAGIC ## 5. Détail du run — table `resultat_backtest` (historisée)
# MAGIC
# MAGIC Le dossier par dossier (`df_result`) : la partition de la date d'inventaire
# MAGIC du run est remplacée, les autres inventaires sont préservés.

# COMMAND ----------

if DELTA_SCHEMA:
    table_resultat = save_result_delta(df_result, DELTA_SCHEMA, d["date_inventaire"])
    print("Détail du run →", table_resultat)
else:
    print("Pas de schéma Delta (widget delta_schema vide) : détail non historisé.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Récapitulatif des sorties (connexion Power BI)

# COMMAND ----------

out_dir = metrics.output_dir(sub="metrics")
print(f"Fichiers DBFS : {out_dir}\n")
if DELTA_SCHEMA:
    print("Tables Delta (connecteur Power BI ▸ Azure Databricks ▸ SQL Warehouse) :")
    for name in tables:
        print(f"  {DELTA_SCHEMA}.metrique_{name}")
    print(f"  {table_resultat}  (détail df_result tête par tête)")
    print("Toutes historisées par run : DATE_INVENTAIRE × PERIMETRE.")
else:
    print("Pas de schéma Delta configuré (EXPORT_DELTA_SCHEMA dans config/profile.py)")
    print("→ Power BI : importer le classeur Excel (onglets par axe + Sommaire) ou les parquet/csv du dossier ci-dessus.")

# Aperçu de la ligne de synthèse exportée (KPI du run).
display(tables["synthese"])

# COMMAND ----------

df_result.unpersist()
