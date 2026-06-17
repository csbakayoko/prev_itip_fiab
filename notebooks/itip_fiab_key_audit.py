# Databricks notebook source
# MAGIC %md
# MAGIC # ITIP-FIAB — Audit de solidité de la clé (read-only)
# MAGIC
# MAGIC But : jauger `key_strict` AVANT de s'y fier — résistance aux collisions,
# MAGIC combinatoire (« combien de clés constructibles »), et expérience de
# MAGIC substitution de garantie (60/64) sur les lignes compte sans garantie.
# MAGIC **Aucune écriture** : pur diagnostic. Lance les fonctions de `core.match.key_audit`
# MAGIC sur les DataFrames nettoyés (mêmes loaders / clean que le pipeline prod).
# MAGIC
# MAGIC ⚠ Avant de lancer : git pull du Repo + `dbutils.library.restartPython()`.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("itip_fiab").getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

from config import db_cfg, tech_cfg
from core.io.load_data import load_cpt_raw, load_mrm_raw
from core.prep.transform import clean_cpt, clean_mrm
from core.match import key_audit

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Préparation : compte + MRM nettoyés (clés construites, aucune écriture)
# MAGIC
# MAGIC `cpt_clean` est POST-imputation : les lignes garantie nulle + D_INVALIDITE
# MAGIC renseignée portent déjà GARANTIE = CODE_GARANTIE_IP (64), cf. clean_cpt. Pour mesurer
# MAGIC l'effet brut de l'imputation, désactiver l'appel `impute_garantie_ip` dans
# MAGIC clean_cpt (passer `ip_code=None`) et rejouer.

# COMMAND ----------

cpt_clean = clean_cpt(load_cpt_raw(spark, db_cfg), tech_cfg).cache()
mrm_clean = clean_mrm(load_mrm_raw(spark, db_cfg), tech_cfg).cache()
print(f"CPT clean : {cpt_clean.count():,} lignes | MRM clean : {mrm_clean.count():,} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Collisions de `key_strict` (clé partagée par >1 ligne = faux appariement potentiel)

# COMMAND ----------

audit_cpt = key_audit.auditer_cle(cpt_clean, "key_strict", "CPT")
audit_mrm = key_audit.auditer_cle(mrm_clean, "key_strict", "MRM")
display(audit_cpt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Combinatoire : « combien de clés je peux construire »
# MAGIC
# MAGIC Max théorique = produit des cardinalités des composantes (rpp, dob,
# MAGIC survenance, garantie, nom). Le taux d'occupation = clés observées / max
# MAGIC théorique mesure le pouvoir discriminant réel.

# COMMAND ----------

card_cpt = key_audit.cardinalite_cle(cpt_clean, prefix="CPT_", label="CPT")
card_mrm = key_audit.cardinalite_cle(mrm_clean, prefix="MRM_", label="MRM")
display(card_cpt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Expérience : substitution de garantie (60 = IT, 64 = IP)
# MAGIC
# MAGIC Pour les lignes compte SANS garantie (résidu non imputé), on force la
# MAGIC garantie à 60 puis 64 (côté compte seulement, MRM garde sa garantie réelle)
# MAGIC et on compte les matchs MRM. La différence révèle, dossier par dossier,
# MAGIC l'hypothèse IT vs IP qui trouve une contrepartie — pour décider d'étendre
# MAGIC l'imputation au-delà de 60/64.

# COMMAND ----------

subst = key_audit.tester_substitution_garantie(
    cpt_clean, mrm_clean, codes=(60, 64), only_null_garantie=True,
)
display(subst)

# COMMAND ----------

print("✅ AUDIT CLÉ OK — collisions, combinatoire, substitution garantie.")
cpt_clean.unpersist()
mrm_clean.unpersist()
