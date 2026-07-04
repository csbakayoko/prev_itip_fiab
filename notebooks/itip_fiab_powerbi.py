# Databricks notebook source
# MAGIC %md
# MAGIC # ITIP-FIAB — Run de production → Power BI
# MAGIC
# MAGIC Notebook **final** : données + config en entrée, tables métriques en sortie.
# MAGIC
# MAGIC Déroulé :
# MAGIC 1. **Setup** — session Spark + paramètres du run (widgets = paramètres du Job,
# MAGIC    défauts `config/profile.py`)
# MAGIC 2. **Pipeline** — chargement → nettoyage → matching → récupérations (N+1, statut NON) → tags
# MAGIC 3. **Synthèse** — contrôles de cohérence console (`chute_coherente`, lignes classées)
# MAGIC 4. **Export Power BI** — les 20 tables métriques écrites en Delta (+ fichiers DBFS),
# MAGIC    puis contrôles inter-tables BLOQUANTS (les onglets doivent se recouper)
# MAGIC 5. **Détail du run** — `df_result` écrit en table Delta `resultat_backtest`,
# MAGIC    historisée par date d'inventaire (2023 et 2024 coexistent)
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
# MAGIC ## 1. Setup — session Spark + paramètres du run
# MAGIC
# MAGIC Le notebook vit dans le repo (dossier Git Databricks) : la racine est sur `sys.path`.
# MAGIC
# MAGIC **Tous les paramètres sont des widgets** (= « base parameters » du Job
# MAGIC Databricks) : le Job surcharge n'importe lequel sans toucher au code.
# MAGIC Un widget vide = défaut de `config/profile.py` (`INVENTAIRES[annee]`).

# COMMAND ----------

from config import ANNEE_INVENTAIRE, CLIENT_NAME, EXPORT_DELTA_SCHEMA, INVENTAIRES
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
)
print(f"Run inventaire {_annee} :", profil)

# ── Cible de l'export Power BI ───────────────────────────────────────────────
# Delta (recommandé) : Power BI se connecte au SQL Warehouse Databricks et lit
# les tables <schema>.itip_metric_<nom>_<perim>. Sans schéma, les fichiers
# parquet/csv sous DBFS restent disponibles (connecteur fichier / import).
# Excel TOUJOURS produit (classeur propre multi-onglets, import fichier Power BI).
DELTA_SCHEMA = _param("delta_schema", "") or None
FORMATS      = ("excel", "delta", "parquet", "csv") if DELTA_SCHEMA else ("excel", "parquet", "csv")

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
# MAGIC Une passe : les 20 tables métriques écrites en Delta + parquet/csv sur DBFS.

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
        print(f"  {DELTA_SCHEMA}.itip_metric_{name}_{metrics._PERIMETRE}")
    print(f"  {table_resultat}  (détail df_result, historisé par DATE_INVENTAIRE)")
else:
    print("Pas de schéma Delta configuré (EXPORT_DELTA_SCHEMA dans config/profile.py)")
    print("→ Power BI : importer le classeur Excel (onglets par axe + Sommaire) ou les parquet/csv du dossier ci-dessus.")

# Aperçu de la ligne de synthèse exportée (KPI du run).
display(tables["synthese"])

# COMMAND ----------

df_result.unpersist()
