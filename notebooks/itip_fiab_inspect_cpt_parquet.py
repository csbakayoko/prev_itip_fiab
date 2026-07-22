# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 ITIP-FIAB — Investigation d'un parquet CPT (read-only)
# MAGIC
# MAGIC But : diagnostiquer les **écarts de noms de colonnes** entre un parquet
# MAGIC candidat (par défaut `CPT_PARQUET_PATH`, l'export officiel
# MAGIC `tetepartete_itip.PARQUET`) et la table Hive de référence
# MAGIC (`db_cfg.cpt_table`, `compteclient.tetepartete_itip`) — la bascule
# MAGIC parquet du pipeline suppose *mêmes colonnes brutes*.
# MAGIC
# MAGIC ⚠ Piège connu : le flux voisin `tetepartete_re.PARQUET` (dossier
# MAGIC `tetepartete_re/prepare/`, à côté de `tetepartete_itip/prepare/`) est un
# MAGIC **autre flux** aux colonnes différentes —
# MAGIC ce notebook sert précisément à valider qu'on pointe le bon export.
# MAGIC
# MAGIC Le notebook :
# MAGIC 1. lit les deux sources et affiche leurs schémas ;
# MAGIC 2. diffe les colonnes (exactes puis **normalisées** : casse / accents /
# MAGIC    espaces / ponctuation) pour repérer les simples renommages ;
# MAGIC 3. vérifie les colonnes **exigées par le pipeline** (MAPPING_CPT +
# MAGIC    clés de dédoublonnage + colonne de tri) et propose pour chaque
# MAGIC    manquante la candidate la plus proche dans le parquet ;
# MAGIC 4. génère le dict de renommage prêt à l'emploi si tout est rattrapable ;
# MAGIC 5. contrôle rapide du CONTENU (visions, clauses) pour valider que c'est
# MAGIC    bien la même donnée et pas seulement les mêmes en-têtes.
# MAGIC
# MAGIC **Aucune écriture** : pur diagnostic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup + widgets

# COMMAND ----------

import difflib
import unicodedata

from config import db_cfg, tech_cfg, MAPPING_CPT, CPT_PARQUET_PATH
from core.runtime import get_spark

spark = get_spark()

dbutils.widgets.text(
    "parquet_path",
    CPT_PARQUET_PATH or "",
    "Chemin du parquet CPT candidat",
)
PARQUET_PATH = dbutils.widgets.get("parquet_path")

# Colonnes brutes dont le pipeline a besoin sur la source CPT :
#   - clés de MAPPING_CPT (rename canonique dans core/prep/transform.py)
#   - clés de dédoublonnage + colonne de tri last-write (config/params.py)
COLONNES_REQUISES = sorted(
    set(MAPPING_CPT) | set(tech_cfg.cpt_dup_keys) | {tech_cfg.cpt_order_col}
)
print(f"{len(COLONNES_REQUISES)} colonnes requises par le pipeline :")
print(COLONNES_REQUISES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lecture des deux sources + schémas bruts

# COMMAND ----------

df_parquet = spark.read.parquet(PARQUET_PATH)
df_hive    = spark.table(db_cfg.cpt_table)

print(f"PARQUET [{PARQUET_PATH}] — {len(df_parquet.columns)} colonnes")
df_parquet.printSchema()

print(f"\nHIVE [{db_cfg.cpt_table}] — {len(df_hive.columns)} colonnes")
df_hive.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Diff exact des colonnes

# COMMAND ----------

cols_parquet = set(df_parquet.columns)
cols_hive    = set(df_hive.columns)

seulement_parquet = sorted(cols_parquet - cols_hive)
seulement_hive    = sorted(cols_hive - cols_parquet)
communes          = sorted(cols_parquet & cols_hive)

print(f"Communes ({len(communes)}) : {communes}\n")
print(f"Seulement dans le PARQUET ({len(seulement_parquet)}) : {seulement_parquet}\n")
print(f"Seulement dans HIVE ({len(seulement_hive)}) : {seulement_hive}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Diff normalisé — repère les renommages (casse / accents / espaces)
# MAGIC
# MAGIC Deux colonnes qui ne diffèrent que par la casse, les accents ou la
# MAGIC ponctuation sont considérées comme le **même champ renommé**.

# COMMAND ----------

def normalise(nom: str) -> str:
    """minuscules, sans accents, tout séparateur → '_' (aligné snake_case Hive)."""
    s = unicodedata.normalize("NFKD", nom).encode("ascii", "ignore").decode()
    s = "".join(c if c.isalnum() else "_" for c in s.lower())
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


norm_parquet = {normalise(c): c for c in cols_parquet}
norm_hive    = {normalise(c): c for c in cols_hive}

# Même champ sous deux graphies → renommage trivial parquet → hive
renommages = {
    norm_parquet[n]: norm_hive[n]
    for n in set(norm_parquet) & set(norm_hive)
    if norm_parquet[n] != norm_hive[n]
}
vrais_ecarts_parquet = sorted(c for c in seulement_parquet if normalise(c) not in norm_hive)
vrais_ecarts_hive    = sorted(c for c in seulement_hive if normalise(c) not in norm_parquet)

print(f"Renommages triviaux parquet → hive ({len(renommages)}) :")
for src, dst in sorted(renommages.items()):
    print(f"    {src!r:60} → {dst!r}")
print(f"\nColonnes du parquet SANS équivalent hive même normalisé ({len(vrais_ecarts_parquet)}) : {vrais_ecarts_parquet}")
print(f"Colonnes hive INTROUVABLES dans le parquet même normalisé ({len(vrais_ecarts_hive)}) : {vrais_ecarts_hive}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Couverture des colonnes REQUISES par le pipeline
# MAGIC
# MAGIC Pour chaque colonne attendue absente du parquet : candidate la plus
# MAGIC proche (normalisation puis rapprochement flou `difflib`).

# COMMAND ----------

manquantes = [c for c in COLONNES_REQUISES if c not in cols_parquet]
presentes  = [c for c in COLONNES_REQUISES if c in cols_parquet]

print(f"Présentes telles quelles : {len(presentes)}/{len(COLONNES_REQUISES)}")

correspondances = {}   # colonne requise → candidate parquet
for col in manquantes:
    n = normalise(col)
    if n in norm_parquet:                      # même champ, autre graphie
        correspondances[col] = norm_parquet[n]
        statut = f"→ {norm_parquet[n]!r} (renommage trivial)"
    else:                                      # rapprochement flou
        proches = difflib.get_close_matches(n, list(norm_parquet), n=3, cutoff=0.6)
        if proches:
            correspondances[col] = norm_parquet[proches[0]]
            statut = f"≈ {[norm_parquet[p] for p in proches]} (flou — À VALIDER)"
        else:
            statut = "✗ AUCUNE candidate"
    print(f"    {col!r:40} {statut}")

if not manquantes:
    print("Toutes les colonnes requises sont présentes : le parquet est utilisable tel quel.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Dict de renommage prêt à l'emploi
# MAGIC
# MAGIC Si toutes les manquantes ont une candidate, ce dict suffit à aligner le
# MAGIC parquet sur les colonnes brutes attendues :
# MAGIC `df.withColumnsRenamed(RENAME_PARQUET)` en amont du pipeline.

# COMMAND ----------

RENAME_PARQUET = {cand: col for col, cand in correspondances.items()}

print("RENAME_PARQUET = {")
for src, dst in sorted(RENAME_PARQUET.items()):
    print(f"    {src!r}: {dst!r},")
print("}")

irrecuperables = [c for c in manquantes if c not in correspondances]
if irrecuperables:
    print(f"\n⚠ Colonnes requises SANS candidate — parquet inutilisable en l'état : {irrecuperables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Contrôle du CONTENU — même donnée derrière les en-têtes ?
# MAGIC
# MAGIC Volumétrie, visions et clauses des deux sources côte à côte (sur les
# MAGIC colonnes disponibles, renommage appliqué au parquet).

# COMMAND ----------

df_parquet_aligne = df_parquet.withColumnsRenamed(RENAME_PARQUET)

print(f"Volumétrie : parquet = {df_parquet_aligne.count():,} lignes | hive = {df_hive.count():,} lignes")

for col in ("vision", "clause"):
    for label, df in (("PARQUET", df_parquet_aligne), ("HIVE", df_hive)):
        if col in df.columns:
            vals = [r[0] for r in df.select(col).distinct().limit(20).collect()]
            print(f"{label:8} {col} (20 premières distinctes) : {sorted(v for v in vals if v is not None)}")
        else:
            print(f"{label:8} {col} : colonne ABSENTE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Échantillon du parquet aligné

# COMMAND ----------

cols_affichees = [c for c in COLONNES_REQUISES if c in df_parquet_aligne.columns]
display(df_parquet_aligne.select(*cols_affichees).limit(10))
