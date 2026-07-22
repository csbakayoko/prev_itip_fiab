# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 ITIP-FIAB — Exploration d'une clause dans les tables brutes
# MAGIC
# MAGIC **Notebook d'investigation, 100 % lecture** : partir d'une clause ciblée
# MAGIC (typiquement une clause PB représentative des anomalies du compte,
# MAGIC identifiée en §4.7 des notebooks de vision 📗 `itip_fiab_vision_cc2023` /
# MAGIC 📘 `itip_fiab_vision_cc2024`) et **taper directement dans les tables
# MAGIC brutes du schéma** pour rassembler tout le contexte : lignes du compte,
# MAGIC colonnes de traçabilité (**qui a réalisé le compte**, dates de saisie),
# MAGIC autres tables du schéma qui portent cette clause.
# MAGIC
# MAGIC Ici, **pas de pipeline ni de rapprochement** : ni mapping, ni clés, ni
# MAGIC métriques — uniquement des requêtes sur les tables existantes, pour
# MAGIC préparer l'échange avec le préparateur du compte (remontée des anomalies
# MAGIC au cas par cas).
# MAGIC
# MAGIC | Widget | Rôle | Exemple |
# MAGIC |---|---|---|
# MAGIC | `clause` | le numéro de clause à investiguer (sans préfixe) | `121981` |
# MAGIC | `vision` | la vision comptable (vide = toutes) | `CC2024` |
# MAGIC | `schema` | le schéma où vivent les tables brutes | `hive_metastore.compteclient` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ⚙️ Setup — session Spark + widgets

# COMMAND ----------

from pyspark.sql import functions as F

from config import db_cfg
from core.runtime import get_spark

spark = get_spark()

dbutils.widgets.text("clause", "",                            "Clause (numéro, ex. 121981)")
dbutils.widgets.text("vision", "",                            "Vision CPT (vide = toutes)")
dbutils.widgets.text("schema", "hive_metastore.compteclient", "Schéma des tables brutes")

CLAUSE = dbutils.widgets.get("clause").strip()
VISION = dbutils.widgets.get("vision").strip()
SCHEMA = dbutils.widgets.get("schema").strip()

assert CLAUSE, "⚠ Renseigner le widget `clause` (numéro sans préfixe, ex. 121981)."
print(f"🔍 Clause investiguée : {CLAUSE}  ·  vision : {VISION or '(toutes)'}  ·  schéma : {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 📄 La table brute du compte — lignes de la clause
# MAGIC
# MAGIC Chargement **brut** de la table compte (`db_cfg.cpt_table`), sans mapping
# MAGIC ni renommage : toutes les colonnes de la source restent visibles. La
# MAGIC colonne brute `clause` porte le préfixe de type (`CPB_121981` pour un
# MAGIC compte PB) : on filtre sur le numéro, préfixe ignoré.

# COMMAND ----------

cpt_brut = spark.table(db_cfg.cpt_table)
print(f"Table {db_cfg.cpt_table} — {len(cpt_brut.columns)} colonnes")

lignes_clause = cpt_brut.filter(F.col("clause").rlike(f"(^|_){CLAUSE}$"))
if VISION:
    lignes_clause = lignes_clause.filter(F.col("vision") == VISION)

nb = lignes_clause.count()
print(f"📄 {nb:,} ligne(s) pour la clause {CLAUSE}" + (f" en vision {VISION}" if VISION else ""))
display(lignes_clause)

# COMMAND ----------

# Visions présentes pour cette clause (utile si le widget vision est vide).
display(
    cpt_brut.filter(F.col("clause").rlike(f"(^|_){CLAUSE}$"))
            .groupBy("vision", "clause").count()
            .orderBy("vision")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 👤 Colonnes de traçabilité — qui a réalisé le compte ?
# MAGIC
# MAGIC L'identité du préparateur n'est pas dans les colonnes utilisées par le
# MAGIC backtest — mais elle peut exister ailleurs dans la table brute ou dans une
# MAGIC autre table du schéma. On repère d'abord les **colonnes candidates** par
# MAGIC leur nom (utilisateur, auteur, gestionnaire, saisie, création,
# MAGIC modification…), puis on affiche leurs valeurs distinctes pour la clause.

# COMMAND ----------

_MOTIFS_TRACABILITE = ("user", "util", "auteur", "gest", "saisi", "creat",
                       "modif", "maj", "agent", "resp", "oper", "matric")

def colonnes_tracabilite(colonnes):
    """Colonnes dont le nom évoque un auteur / une trace de saisie."""
    return [c for c in colonnes if any(m in c.lower() for m in _MOTIFS_TRACABILITE)]

cols_trace = colonnes_tracabilite(cpt_brut.columns)
print(f"👤 Colonnes candidates dans {db_cfg.cpt_table} : {cols_trace or 'aucune — voir les autres tables du schéma (§4-5)'}")

if cols_trace and nb:
    for c in cols_trace:
        print(f"\n— {c} (valeurs distinctes pour la clause) :")
        display(lignes_clause.groupBy(c).count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 🗂️ Les autres tables du schéma
# MAGIC
# MAGIC Inventaire de ce qui existe autour de la table compte : c'est là que
# MAGIC peuvent vivre les informations absentes du flux (référentiel des clauses,
# MAGIC suivi des imports, traçabilité des comptes…).

# COMMAND ----------

tables_schema = [r.tableName for r in spark.sql(f"SHOW TABLES IN {SCHEMA}").collect()]
print(f"🗂️ {len(tables_schema)} table(s) dans {SCHEMA} :")
for t in tables_schema:
    print(f"   · {t}")

# COMMAND ----------

# Structure d'une table du schéma (changer le nom pour inspecter une autre).
display(spark.sql(f"DESCRIBE TABLE {SCHEMA}.{tables_schema[0]}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 🧭 Où retrouve-t-on la clause ? — balayage du schéma
# MAGIC
# MAGIC Pour chaque table du schéma : repérage des colonnes dont le nom contient
# MAGIC « clause », comptage des lignes qui portent notre numéro, et relevé des
# MAGIC colonnes de traçabilité disponibles. Le tableau final dit **dans quelles
# MAGIC tables creuser** (les tables illisibles sont signalées, pas bloquantes).

# COMMAND ----------

import pandas as pd

recap = []
for t in tables_schema:
    nom = f"{SCHEMA}.{t}"
    try:
        df_t       = spark.table(nom)
        cols_cl    = [c for c in df_t.columns if "clause" in c.lower()]
        cols_tr    = colonnes_tracabilite(df_t.columns)
        nb_lignes  = 0
        if cols_cl:
            cond = None
            for c in cols_cl:
                m = F.col(c).cast("string").rlike(f"(^|_){CLAUSE}$")
                cond = m if cond is None else (cond | m)
            nb_lignes = df_t.filter(cond).count()
        recap.append({
            "TABLE"              : t,
            "COLONNES_CLAUSE"    : ", ".join(cols_cl) or "—",
            "NB_LIGNES_CLAUSE"   : nb_lignes,
            "COLONNES_TRACABILITE": ", ".join(cols_tr) or "—",
            "LECTURE"            : "OK",
        })
    except Exception as exc:  # table illisible (droits, format…) : on trace, on continue
        recap.append({
            "TABLE": t, "COLONNES_CLAUSE": "—", "NB_LIGNES_CLAUSE": 0,
            "COLONNES_TRACABILITE": "—", "LECTURE": f"KO — {type(exc).__name__}",
        })

recap_pdf = (pd.DataFrame(recap)
               .sort_values("NB_LIGNES_CLAUSE", ascending=False)
               .reset_index(drop=True))
display(recap_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 🧪 Requêtes libres — creuser une piste
# MAGIC
# MAGIC Cellules modèles à adapter : le numéro de clause et le schéma des widgets
# MAGIC sont injectés. Dupliquer la cellule, changer la table / les colonnes au
# MAGIC fil de l'investigation.

# COMMAND ----------

# Modèle 1 — toutes les lignes d'une table portant la clause.
_table = tables_schema[0]  # ← remplacer par la table repérée en §5
_col   = "clause"          # ← remplacer par la colonne repérée en §5
display(spark.sql(f"""
    SELECT *
    FROM   {SCHEMA}.{_table}
    WHERE  CAST({_col} AS STRING) RLIKE '(^|_){CLAUSE}$'
"""))

# COMMAND ----------

# Modèle 2 — profil d'une colonne (valeurs distinctes + volumétrie) sur la clause.
# display(spark.sql(f"""
#     SELECT <colonne>, COUNT(*) AS nb
#     FROM   {SCHEMA}.<table>
#     WHERE  CAST(<colonne_clause> AS STRING) RLIKE '(^|_){CLAUSE}$'
#     GROUP  BY <colonne>
#     ORDER  BY nb DESC
# """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. ✅ Rappels
# MAGIC
# MAGIC - **Aucune écriture** : ce notebook ne modifie rien, il lit.
# MAGIC - Les anomalies se remontent **au cas par cas au préparateur du compte**
# MAGIC   (pas de circuit de correction centralisé) — cf. cartographie des
# MAGIC   anomalies v1.2, section « éclairage à la source ».
# MAGIC - Les clauses cibles (2 premières en volumétrie de dossiers, 2 premières
# MAGIC   en poids de PM) sortent de la section 4.7 des notebooks de vision.
