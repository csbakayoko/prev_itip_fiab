# Databricks notebook source
# MAGIC %md
# MAGIC # 📎 ITIP-FIAB — Extraction des lignes tête par tête à investiguer
# MAGIC
# MAGIC **Notebook de préparation des échanges avec les préparateurs de comptes.**
# MAGIC Il produit, pour chaque clause ciblée, **un fichier Excel autonome** qui
# MAGIC contient les lignes tête par tête du compte restées **sans contrepartie
# MAGIC MRM** (anomalies définitives, `CPT_ONLY`) — la pièce jointe à envoyer à la
# MAGIC personne qui a réalisé le compte.
# MAGIC
# MAGIC Les clauses ciblées sont celles de la **section 4.7** des notebooks de
# MAGIC vision (📗 `itip_fiab_vision_cc2023` / 📘 `itip_fiab_vision_cc2024`) : les
# MAGIC **2 premières clauses PB en volumétrie de dossiers** et les **2 premières
# MAGIC en poids de PM**. Widget `clauses` vide ⇒ elles sont recalculées ici, à
# MAGIC l'identique.
# MAGIC
# MAGIC | Widget | Rôle | Défaut |
# MAGIC |---|---|---|
# MAGIC | `annee_inventaire` | la vision à rejouer (2023 / 2024) | `2023` |
# MAGIC | `clauses` | clauses à extraire, séparées par des virgules (vide = les 4 cibles) | *(vide)* |
# MAGIC | `dossier_export` | où déposer les classeurs | `/dbfs/FileStore/itip_fiab/investigation` |
# MAGIC
# MAGIC > ⚠ Ce notebook **n'écrit aucune table** : il ne dépose que les fichiers
# MAGIC > d'investigation demandés. L'identité du préparateur du compte ne vient
# MAGIC > pas d'ici — elle se cherche dans les tables brutes, avec 🔍
# MAGIC > `itip_fiab_exploration_clauses`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ⚙️ Setup — session Spark, vision, clauses cibles

# COMMAND ----------

import os
import unicodedata

import pandas as pd
import pyspark.sql.functions as F

from config import INVENTAIRES
from core.runtime import configurer_run, get_spark
from core import metrics
from main import build_df_result

spark = get_spark()

dbutils.widgets.text("annee_inventaire", "2023", "Année d'inventaire")
dbutils.widgets.text("clauses",          "",     "Clauses (vide = les 4 cibles)")
dbutils.widgets.text("dossier_export",   "/dbfs/FileStore/itip_fiab/investigation",
                     "Dossier d'export")

ANNEE   = dbutils.widgets.get("annee_inventaire").strip()
CLAUSES = [c.strip() for c in dbutils.widgets.get("clauses").split(",") if c.strip()]
EXPORT  = dbutils.widgets.get("dossier_export").rstrip("/")

_inv   = INVENTAIRES[ANNEE]
profil = configurer_run(
    date_inventaire = _inv["date"],
    cpt_vision      = _inv["vision"],
    fichier_mrm     = _inv["mrm"],
    fichier_mrm_n1  = _inv["mrm_n1"] or None,
)
print(f"📎 Extraction — vision {profil['cpt_vision']} au {profil['date_inventaire']}")

# COMMAND ----------

df_result = build_df_result(spark).persist()
print(f"🏗️  df_result : {df_result.count():,} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Les clauses cibles
# MAGIC
# MAGIC Même règle qu'en §4.7 des notebooks de vision : parmi les anomalies du
# MAGIC compte, les 2 premières clauses **PB** en nombre de dossiers et les 2
# MAGIC premières en PM (2 à 4 clauses selon recouvrement). Les poids se lisent en
# MAGIC **part du total des anomalies**, tous types de compte confondus.

# COMMAND ----------

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

if CLAUSES:                                   # widget renseigné : il fait foi
    cibles_pb = cibles_pb[cibles_pb["CLAUSE"].astype(str).isin(CLAUSES)].reset_index(drop=True)

print(f"🎯 {len(cibles_pb)} clause(s) à extraire : "
      f"{', '.join(cibles_pb['CLAUSE'].astype(str))}")
display(cibles_pb)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 🧾 Les lignes tête par tête des anomalies
# MAGIC
# MAGIC Une ligne = un dossier du compte **absent de la revue MRM** (ni dans
# MAGIC l'inventaire courant, ni récupéré, ni repêché). On garde les colonnes qui
# MAGIC permettent au préparateur de **reconnaître le dossier dans ses propres
# MAGIC outils** : identité de l'assuré, dates, garantie, état, montants — plus
# MAGIC deux colonnes d'aide à l'analyse :
# MAGIC
# MAGIC - `MOTIF_PROBABLE` — la segmentation des anomalies (déclaration tardive
# MAGIC   probable / montant élevé / à analyser) ;
# MAGIC - `COMPOSANTES_CLE_NULLES` — les éléments d'identification manquants
# MAGIC   (RPP, clause, dates…) : quand l'un d'eux est vide, le rapprochement est
# MAGIC   **impossible par construction**, c'est souvent là qu'est la réponse.

# COMMAND ----------

# Colonnes de la pièce jointe : nom technique → libellé lisible. Une colonne
# absente du run (source qui évolue) est simplement ignorée.
COLONNES_PJ = {
    "CLAUSE"                  : "Clause",
    "TYPE_COMPTE"             : "Type de compte",
    "CPT_RPP"                 : "N° RPP",
    "CPT_NOM_PRENOM"          : "Nom et prénom",
    "CPT_D_NAISSANCE"         : "Date de naissance",
    "CPT_D_SURVENANCE"        : "Date d'arrêt de travail",
    "CPT_EXERCICE"            : "Exercice de survenance",
    "CPT_GARANTIE"            : "Terme de garantie",
    "CPT_D_INVALIDITE"        : "Date de mise en invalidité",
    "CPT_CATEGORIE_INVALIDITE": "Catégorie d'invalidité",
    "CPT_ETAT_DOSSIER"        : "État du dossier à l'extraction",
    "CPT_PM"                  : "PM au 31/12",
    "CPT_PSAP"                : "PSAP au 31/12",
    "CPT_VISION"              : "Vision comptable",
    "CPT_TECH_DAY"            : "Jour d'extraction",
    "MOTIF_PROBABLE"          : "Motif probable",
    "COMPOSANTES_CLE_NULLES"  : "Éléments d'identification manquants",
}

# Éléments d'identification qui servent au rapprochement : un seul vide et le
# dossier ne peut pas être retrouvé (la clé les concatène en ignorant les vides).
CLE_COMPOSANTES = {
    "CPT_RPP"         : "RPP",
    "CPT_NOM_PRENOM"  : "nom/prénom",
    "CPT_D_NAISSANCE" : "date de naissance",
    "CPT_D_SURVENANCE": "date d'arrêt",
    "CPT_GARANTIE"    : "garantie",
    "CPT_CLAUSE"      : "clause",
}


def lignes_anomalies(df_result, clause: str):
    """Lignes tête par tête du compte sans contrepartie MRM, pour une clause."""
    df = df_result.filter(
        (F.col("TYPE_RECONCILIATION") == "CPT_ONLY")
        & (F.col("CLAUSE").cast("string") == F.lit(str(clause)))
    )

    df = (
        df.withColumn("MOTIF_PROBABLE", F.col("TAG_CPT_ONLY"))
          .withColumn(
              "COMPOSANTES_CLE_NULLES",
              F.concat_ws(", ", *[
                  F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""),
                         F.lit(lib))
                  for c, lib in CLE_COMPOSANTES.items() if c in df.columns
              ]),
          )
    )

    cols = [c for c in COLONNES_PJ if c in df.columns]
    pdf  = df.select(*cols).toPandas().rename(columns=COLONNES_PJ)
    if "PM au 31/12" in pdf.columns:
        pdf = pdf.sort_values("PM au 31/12", ascending=False)
    return pdf.reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 📦 Un classeur par clause
# MAGIC
# MAGIC Chaque classeur porte deux onglets : **« Lignes à investiguer »** (la
# MAGIC pièce jointe proprement dite) et **« Repères »** (le poids de la clause
# MAGIC dans les anomalies + le rappel du périmètre du run, pour que le fichier se
# MAGIC lise seul, sans le corps du message).

# COMMAND ----------


def _slug(valeur: str) -> str:
    """Nom de fichier sûr (sans accent ni espace)."""
    txt = unicodedata.normalize("NFKD", str(valeur)).encode("ascii", "ignore").decode()
    return "".join(c if c.isalnum() else "_" for c in txt).strip("_")


def reperes(ligne, nb_lignes: int) -> pd.DataFrame:
    """Onglet de contexte du classeur — le fichier doit se lire seul."""
    return pd.DataFrame([
        ("Clause",                          str(ligne["CLAUSE"])),
        ("Type de compte",                  ligne["TYPE_COMPTE"]),
        ("Vision comptable",                profil["cpt_vision"]),
        ("Date d'inventaire",               profil["date_inventaire"]),
        ("Revue de référence",              os.path.basename(profil["fichier_mrm"])),
        ("Revue suivante interrogée",       os.path.basename(profil["fichier_mrm_n1"])
                                            if profil["fichier_mrm_n1"] else "—"),
        ("Dossiers sans contrepartie",      f"{nb_lignes:,}".replace(",", " ")),
        ("PM concernée (€)",                f"{ligne['PM_CPT']:,.0f}".replace(",", " ")),
        ("Part des anomalies (dossiers)",   f"{ligne['POIDS_NB_PCT']} %"),
        ("Part des anomalies (PM)",         f"{ligne['POIDS_PM_PCT']} %"),
        ("Critère de sélection",            ligne["CRITERE"]),
    ], columns=["Repère", "Valeur"])


os.makedirs(EXPORT, exist_ok=True)
fichiers = []

for _, ligne in cibles_pb.iterrows():
    clause = str(ligne["CLAUSE"])
    pdf    = lignes_anomalies(df_result, clause)
    chemin = f"{EXPORT}/anomalies_compte_{profil['cpt_vision']}_clause_{_slug(clause)}.xlsx"

    with pd.ExcelWriter(chemin, engine="openpyxl") as writer:
        pdf.to_excel(writer, sheet_name="Lignes à investiguer", index=False)
        reperes(ligne, len(pdf)).to_excel(writer, sheet_name="Repères", index=False)

    fichiers.append({
        "CLAUSE"      : clause,
        "NB_LIGNES"   : len(pdf),
        "PM_CPT"      : round(float(pdf["PM au 31/12"].sum()), 2) if len(pdf) else 0.0,
        "FICHIER"     : chemin,
        "TELECHARGER" : chemin.replace("/dbfs/FileStore", "/files"),
    })
    print(f"✔ Clause {clause} — {len(pdf):,} ligne(s) → {chemin}")

recap = pd.DataFrame(fichiers)
display(recap)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ⬇️ Récupérer les classeurs
# MAGIC
# MAGIC Les liens ci-dessous téléchargent les fichiers depuis l'espace de travail
# MAGIC (préfixer par l'adresse de l'espace si le clic direct ne suffit pas).

# COMMAND ----------

displayHTML(
    "<div style=\"font-family:'Segoe UI',sans-serif;font-size:13px\">"
    "<b>📎 Pièces jointes prêtes</b><ul>"
    + "".join(
        f"<li><a href='{r['TELECHARGER']}'>Clause {r['CLAUSE']}</a> — "
        f"{r['NB_LIGNES']:,} dossier(s), PM {r['PM_CPT']:,.0f} €</li>"
        for _, r in recap.iterrows()
    )
    + "</ul></div>"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 🔍 Contexte complémentaire par clause *(optionnel)*
# MAGIC
# MAGIC Répartition des anomalies de la clause par motif probable et par état de
# MAGIC dossier — de quoi calibrer les questions posées au préparateur (un lot
# MAGIC massivement « fin d'année » ne raconte pas la même histoire qu'un lot
# MAGIC d'identifiants incomplets).

# COMMAND ----------

for _, ligne in cibles_pb.iterrows():
    pdf = lignes_anomalies(df_result, str(ligne["CLAUSE"]))
    if pdf.empty:
        continue
    print(f"\n── Clause {ligne['CLAUSE']} — {len(pdf):,} dossier(s)")
    for col in ("Motif probable", "État du dossier à l'extraction",
                "Éléments d'identification manquants"):
        if col in pdf.columns:
            print(f"\n  {col} :")
            print(pdf[col].fillna("—").replace("", "—")
                     .value_counts().to_string().replace("\n", "\n  "))

# COMMAND ----------

df_result.unpersist()
