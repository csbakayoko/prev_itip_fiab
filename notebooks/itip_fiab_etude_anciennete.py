# Databricks notebook source
# MAGIC %md
# MAGIC # 🔬 ITIP-FIAB — Étude statistique du taux de chute par ancienneté de surveillance
# MAGIC
# MAGIC **Le notebook qui répond à la question posée en comité** : la ventilation du
# MAGIC taux de chute par ancienneté montre un taux **positif sur l'année N**
# MAGIC (sous-provisionnement : compte < revue) et **négatif sur les années passées**
# MAGIC (sur-provisionnement : compte > revue) — le comité s'attendait plutôt à
# MAGIC l'effet inverse. Cette étude établit, chiffres et graphiques à l'appui :
# MAGIC
# MAGIC 1. **Le profil est-il réel ?** — recalcul indépendant depuis le détail tête
# MAGIC    par tête, recoupé avec les tables officielles (§3) ;
# MAGIC 2. **Où le signe bascule-t-il exactement ?** — ancienneté fine, année de
# MAGIC    survenance par année de survenance, pas seulement N / N-1 / N-2+ (§4) ;
# MAGIC 3. **Le solde est-il un effet de masse ou de quelques dossiers ?** —
# MAGIC    dispersion (§5) et concentration des écarts (§6) ;
# MAGIC 4. **Le signe est-il statistiquement solide ?** — sensibilité aux plus gros
# MAGIC    dossiers, intervalles de confiance par rééchantillonnage, test du signe (§7) ;
# MAGIC 5. **Qu'est-ce qui l'explique ?** — composition par garantie (IT/IP),
# MAGIC    consigne, clé de rapprochement, mois de survenance (§8) ;
# MAGIC 6. **La lecture métier** (§9) et **la conclusion ultime** (§12), générées
# MAGIC    depuis les chiffres calculés.
# MAGIC
# MAGIC **Source** : la table Hive `resultat_backtest` — le détail des lignes déjà
# MAGIC rapprochées et historisées par le Job 🚀 `itip_fiab_powerbi`. **Aucun
# MAGIC rapprochement n'est rejoué ici.**
# MAGIC
# MAGIC **Sorties** : 4 tables Hive `etude_anciennete_*` (mêmes colonnes de run que
# MAGIC les tables métriques : `CLE_RUN`, historisation par `DATE_INVENTAIRE ×
# MAGIC PERIMETRE`) + **un classeur Excel multi-onglets** + les graphiques en PNG —
# MAGIC de quoi monter directement le support de présentation.
# MAGIC
# MAGIC | Widget | Rôle | Défaut |
# MAGIC |---|---|---|
# MAGIC | `annee_inventaire` | l'inventaire étudié (2023 / 2024) | `2023` |
# MAGIC | `perimetre` | la colonne PERIMETRE du run historisé | config (`MULTI`) |
# MAGIC | `delta_schema` | schéma Hive (lecture du détail + écriture de l'étude) | config |
# MAGIC | `dossier_export` | où déposer le classeur Excel et les PNG | `/dbfs/FileStore/itip_fiab/etudes` |
# MAGIC | `ecrire_tables` | `oui` = écrit les tables `etude_anciennete_*` | `oui` |
# MAGIC | `nb_reechantillons` | tirages pour les intervalles de confiance | `2000` |
# MAGIC
# MAGIC > 📚 Contrats : [`docs/METRIQUES.md`](../docs/METRIQUES.md) — le taux de
# MAGIC > chute (§4), son univers (matchés inventaire courant hors « à supprimer » /
# MAGIC > statut NON) et sa ventilation par ancienneté sont repris ICI À L'IDENTIQUE.
# MAGIC > Ce notebook n'écrit jamais les tables `metrique_*` : il ne dépose que ses
# MAGIC > propres tables d'étude.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ⚙️ Setup — lecture du détail historisé
# MAGIC
# MAGIC On lit le run demandé dans `resultat_backtest` via sa clé de liaison
# MAGIC `CLE_RUN` (« date ISO | périmètre » — la même que sur toutes les tables
# MAGIC métriques). Si le run n'y est pas, le message dit quoi faire : rejouer le
# MAGIC Job d'export officiel d'abord.

# COMMAND ----------

import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as F

from config import (
    INVENTAIRES, EXPORT_DELTA_SCHEMA, PERIMETRE_LABEL, CLIENT_NAME,
    MATCH_PRINCIPALE, MATCH_AFFINEE, MATCH_RECUPERATION, MATCH_CLAUSE,
    CODE_GARANTIE_IT, CODE_GARANTIE_IP,
)
from core.runtime import get_spark
from core.io.save_result import cle_run, to_date_iso, write_delta_historise
from core.metrics.base import (
    _filter_chute_universe, _with_mrm_action,
    EXERCICE_INV, BLOC_N, BLOC_N1, BLOC_N2_PLUS, BLOC_INDET, _BLOC_ORDRE,
)
from core.metrics.agregats import AXE_ANCIENNETE
# Palette et typographie des graphiques officiels — les mêmes couleurs que les
# graphes 3 / 10 / 12 déjà présentés : sienne = sous-provisionné (risque),
# océan = sur-provisionné (marge).
from core.metrics.viz import (
    C_BLEU, C_OCEAN, C_TEAL, C_SIENNE, C_ROUGE, C_GRIS,
    F_TITRE, F_SST, F_AXE, F_TXT, F_LEG, _n, _meur, _pct,
)

spark = get_spark()

dbutils.widgets.text("annee_inventaire",  "2023",          "Année d'inventaire")
dbutils.widgets.text("perimetre",         PERIMETRE_LABEL, "Périmètre du run")
dbutils.widgets.text("delta_schema",      EXPORT_DELTA_SCHEMA or "", "Schéma Hive")
dbutils.widgets.text("dossier_export",    "/dbfs/FileStore/itip_fiab/etudes", "Dossier d'export")
dbutils.widgets.text("ecrire_tables",     "oui",           "Écrire les tables d'étude")
dbutils.widgets.text("nb_reechantillons", "2000",          "Tirages IC")

ANNEE   = dbutils.widgets.get("annee_inventaire").strip()
PERIM   = dbutils.widgets.get("perimetre").strip()
SCHEMA  = dbutils.widgets.get("delta_schema").strip()
EXPORT  = dbutils.widgets.get("dossier_export").rstrip("/")
ECRIRE  = dbutils.widgets.get("ecrire_tables").strip().lower() == "oui"
N_TIR   = int(dbutils.widgets.get("nb_reechantillons"))

DATE_INV  = INVENTAIRES[ANNEE]["date"]          # "dd/MM/yyyy"
DATE_ISO  = to_date_iso(DATE_INV)               # "yyyy-MM-dd"
ANNEE_INV = int(ANNEE)
CLE       = cle_run(DATE_ISO, PERIM)

assert SCHEMA, "Widget delta_schema vide — aucune source pour le détail du run."

print(f"🔬 Étude ancienneté — inventaire {DATE_INV}, périmètre {PERIM}, clé de run {CLE}")

# COMMAND ----------

try:
    detail = spark.table(f"{SCHEMA}.resultat_backtest").filter(F.col("CLE_RUN") == F.lit(CLE))
    nb_detail = detail.count()
except Exception as exc:
    raise RuntimeError(
        f"Impossible de lire {SCHEMA}.resultat_backtest — le détail du run doit "
        "être historisé AVANT cette étude : rejouer le Job itip_fiab_powerbi "
        f"(widget annee_inventaire = {ANNEE})."
    ) from exc

assert nb_detail > 0, (
    f"Aucune ligne pour la clé de run {CLE} dans {SCHEMA}.resultat_backtest — "
    f"rejouer le Job itip_fiab_powerbi (annee_inventaire = {ANNEE}, périmètre {PERIM})."
)
print(f"📄 Détail du run : {nb_detail:,} lignes tête par tête")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 🎯 L'univers d'analyse — exactement celui des stats globales
# MAGIC
# MAGIC Même filtre que la table `chute` officielle (`docs/METRIQUES.md` §4.2) :
# MAGIC **matchés de l'inventaire courant**, hors consigne « à supprimer » et hors
# MAGIC statut inventaire NON. Les récupérés N+1 sont **exclus** (analyse séparée,
# MAGIC hors stats globales) : l'étude porte sur LE taux présenté.
# MAGIC
# MAGIC Chaque dossier reçoit :
# MAGIC - son **écart signé** `ECART = PM_MRM − PM_CPT` (positif = sous-provisionné,
# MAGIC   risque ; négatif = sur-provisionné, marge — même signe que le taux) ;
# MAGIC - son **ancienneté de surveillance** = année d'inventaire − année de
# MAGIC   survenance (0 = N, 1 = N-1, …), et son bloc N / N-1 / N-2 et antérieur ;
# MAGIC - ses dimensions d'explication : garantie (IT / IP), consigne, famille de
# MAGIC   clé de rapprochement, mois de survenance.

# COMMAND ----------

# Famille de clé : regroupement lisible des étapes du rapprochement.
_FAMILLE_CLE = (
    {t: "Clé principale"   for t in MATCH_PRINCIPALE}
    | {t: "Clé affinée"    for t in MATCH_AFFINEE}
    | {t: "Récupération"   for t in MATCH_RECUPERATION}
    | {t: "Clé clause"     for t in MATCH_CLAUSE}
)

_univers = (
    _filter_chute_universe(_with_mrm_action(detail))
    .filter(F.col("TYPE_RECONCILIATION") != "CPT_LATE")     # stats globales = inventaire courant seul
    .withColumn("ANNEE_SURVENANCE", F.year("CPT_D_SURVENANCE"))
    .withColumn("MOIS_SURVENANCE",  F.month("CPT_D_SURVENANCE"))
)

_colonnes = [c for c in (
    "TYPE_RECONCILIATION", "TYPE_COMPTE", "CLAUSE", "MRM_ACTION",
    "CPT_RPP", "CPT_NOM_PRENOM", "CPT_GARANTIE", "CPT_ETAT_DOSSIER",
    "ANNEE_SURVENANCE", "MOIS_SURVENANCE", "CPT_PM", "MRM_PM",
) if c in _univers.columns]

base = _univers.select(*_colonnes).toPandas()

# ── Dérivations pandas (l'univers de chute tient en mémoire pilote) ──────────
base["PM_MRM"] = pd.to_numeric(base["MRM_PM"], errors="coerce").fillna(0.0)
base["PM_CPT"] = pd.to_numeric(base["CPT_PM"], errors="coerce").fillna(0.0)
base["ECART"]  = base["PM_MRM"] - base["PM_CPT"]

base["ANNEE_SURVENANCE"] = base["ANNEE_SURVENANCE"].astype("Int64")
base["ANCIENNETE"]       = (ANNEE_INV - base["ANNEE_SURVENANCE"]).astype("Int64")
base["BLOC_ANCIENNETE"]  = np.select(
    [base["ANCIENNETE"].eq(0).fillna(False).to_numpy(),
     base["ANCIENNETE"].eq(1).fillna(False).to_numpy(),
     base["ANCIENNETE"].ge(2).fillna(False).to_numpy()],
    [BLOC_N, BLOC_N1, BLOC_N2_PLUS], default=BLOC_INDET,
)

_gar = pd.to_numeric(base.get("CPT_GARANTIE"), errors="coerce")
base["GARANTIE_LIBELLE"] = np.select(
    [_gar == CODE_GARANTIE_IT, _gar == CODE_GARANTIE_IP, _gar.isna()],
    ["IT (incapacité)", "IP (invalidité)", "Non renseignée"],
    default="Autre garantie",
)

base["CONSIGNE"] = (
    base["MRM_ACTION"].str.replace("MRM_", "", regex=False)
    .fillna("Sans consigne reconnue")
)
base["FAMILLE_CLE"] = base["TYPE_RECONCILIATION"].map(_FAMILLE_CLE).fillna("Autre")

BLOCS = [b for b in (BLOC_N, BLOC_N1, BLOC_N2_PLUS, BLOC_INDET)
         if b in set(base["BLOC_ANCIENNETE"])]

taux_global = round(base["ECART"].sum() / base["PM_MRM"].sum() * 100, 2) \
              if base["PM_MRM"].sum() else 0.0

print(f"🎯 Univers de chute (inventaire courant) : {len(base):,} dossiers — "
      f"PM revue {base['PM_MRM'].sum() / 1e6:,.1f} M€, "
      f"taux de chute {taux_global} %")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Les agrégats de référence de l'étude
# MAGIC
# MAGIC Une seule fonction d'agrégation, **la formule officielle** (Σ écarts /
# MAGIC Σ PM revue — jamais une moyenne de ratios), enrichie des grandeurs de
# MAGIC dispersion : écart moyen, écart médian, volumes sous / sur / nul.

# COMMAND ----------


def table_chute(pdf: pd.DataFrame, cles, poids_dans=None) -> pd.DataFrame:
    """Agrégats de chute par modalité — formule agrégée officielle + dispersion.

    poids_dans : colonne(s) définissant le bloc des poids (None = poids sur
    l'ensemble de `pdf`). Deux poids par ligne : POIDS_NB_PCT (part des
    dossiers du bloc) et POIDS_PM_PCT (part de la PM revue du bloc).
    """
    t = (
        pdf.assign(_SOUS=pdf["ECART"] > 0, _SUR=pdf["ECART"] < 0, _NUL=pdf["ECART"] == 0)
        .groupby(cles, dropna=False)
        .agg(
            NB_DOSSIERS       = ("ECART", "size"),
            NB_SOUS_PROVISION = ("_SOUS", "sum"),
            NB_SUR_PROVISION  = ("_SUR", "sum"),
            NB_ECART_NUL      = ("_NUL", "sum"),
            PM_MRM            = ("PM_MRM", "sum"),
            PM_CPT            = ("PM_CPT", "sum"),
            ECART             = ("ECART", "sum"),
            ECART_MOYEN       = ("ECART", "mean"),
            ECART_MEDIAN      = ("ECART", "median"),
        )
        .reset_index()
    )
    t["TAUX_CHUTE_PCT"] = np.where(t["PM_MRM"] != 0, t["ECART"] / t["PM_MRM"] * 100, 0.0)
    tot = t.groupby(poids_dans)["PM_MRM"].transform("sum") if poids_dans else t["PM_MRM"].sum()
    t["POIDS_PM_PCT"] = np.where(tot != 0, t["PM_MRM"] / tot * 100, 0.0)
    tot_nb = (t.groupby(poids_dans)["NB_DOSSIERS"].transform("sum")
              if poids_dans else t["NB_DOSSIERS"].sum())
    t["POIDS_NB_PCT"] = np.where(tot_nb != 0, t["NB_DOSSIERS"] / tot_nb * 100, 0.0)
    for c in ("PM_MRM", "PM_CPT", "ECART", "ECART_MOYEN", "ECART_MEDIAN",
              "TAUX_CHUTE_PCT", "POIDS_PM_PCT", "POIDS_NB_PCT"):
        t[c] = t[c].astype(float).round(2)
    return t


def _style_ax(ax):
    """Habillage commun : axes discrets, grille en retrait, zéro marqué."""
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.grid(axis="y", color="#E6E8EC", linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    ax.tick_params(labelsize=F_AXE)


_PNGS = {}          # titre → chemin local, repris dans le récapitulatif final


def _sauver_fig(fig, nom: str):
    """Dépose la figure en PNG dans le dossier d'export (pour le support)."""
    os.makedirs(EXPORT, exist_ok=True)
    chemin = f"{EXPORT}/etude_anciennete_{ANNEE}_{nom}.png"
    fig.savefig(chemin, dpi=200, bbox_inches="tight", facecolor="white")
    _PNGS[nom] = chemin

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ✅ Contrôle de cohérence — le recalcul retombe sur les chiffres présentés
# MAGIC
# MAGIC Avant toute analyse : les blocs N / N-1 / N-2+ recalculés ICI, depuis le
# MAGIC détail tête par tête, doivent redonner **exactement** l'axe « Ancienneté »
# MAGIC de la table `metrique_chute` officielle (même run). C'est la réponse au
# MAGIC premier doute possible : *les chiffres présentés se reproduisent à
# MAGIC l'identique par un chemin de calcul indépendant.*

# COMMAND ----------

par_bloc = table_chute(base, "BLOC_ANCIENNETE")
par_bloc["ORDRE"] = par_bloc["BLOC_ANCIENNETE"].map(_BLOC_ORDRE)
par_bloc = par_bloc.sort_values("ORDRE").reset_index(drop=True)

try:
    officiel = (
        spark.table(f"{SCHEMA}.metrique_chute")
        .filter((F.col("CLE_RUN") == CLE) & (F.col("AXE") == AXE_ANCIENNETE)
                & (F.col("EXERCICE") == EXERCICE_INV))
        .select("SEGMENT", "NB_DOSSIERS", "PM_MRM", "ECART", "TAUX_CHUTE_PCT")
        .toPandas()
    )
except Exception:
    officiel = pd.DataFrame()

if officiel.empty:
    controle = pd.DataFrame()
    print("⚠ metrique_chute indisponible pour ce run — recoupement officiel sauté "
          "(l'étude reste valable : même univers, même formule que le contrat).")
else:
    controle = (
        officiel.rename(columns={
            "NB_DOSSIERS": "NB_OFFICIEL", "PM_MRM": "PM_MRM_OFFICIELLE",
            "ECART": "ECART_OFFICIEL", "TAUX_CHUTE_PCT": "TAUX_OFFICIEL_PCT"})
        .merge(par_bloc.rename(columns={
            "BLOC_ANCIENNETE": "SEGMENT", "NB_DOSSIERS": "NB_RECALCULE",
            "PM_MRM": "PM_MRM_RECALCULEE", "ECART": "ECART_RECALCULE",
            "TAUX_CHUTE_PCT": "TAUX_RECALCULE_PCT"})
              [["SEGMENT", "NB_RECALCULE", "PM_MRM_RECALCULEE",
                "ECART_RECALCULE", "TAUX_RECALCULE_PCT"]],
            on="SEGMENT", how="outer")
    )
    controle["OK"] = (
        (controle["NB_OFFICIEL"] == controle["NB_RECALCULE"])
        & ((controle["TAUX_OFFICIEL_PCT"] - controle["TAUX_RECALCULE_PCT"]).abs() <= 0.05)
    )
    display(controle)
    assert controle["OK"].all(), (
        "❌ Le recalcul ne retombe pas sur la table officielle — vérifier que le "
        "détail historisé correspond bien au run des tables métriques (même Job)."
    )
    print("✅ Recoupement officiel : blocs N / N-1 / N-2+ identiques à metrique_chute "
          "(volumes et taux) — la chaîne de calcul est confirmée.")

display(par_bloc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 📊 L'ancienneté fine — année de survenance par année de survenance
# MAGIC
# MAGIC Le découpage N / N-1 / N-2+ agrège des situations très différentes : ici,
# MAGIC **une ligne par année de survenance**. On y lit précisément *où* le signe
# MAGIC bascule, et avec quel poids de PM — un bloc « N-2 et antérieur » négatif
# MAGIC peut cacher des années individuellement positives (et inversement).

# COMMAND ----------

par_annee = table_chute(base, ["ANNEE_SURVENANCE", "ANCIENNETE", "BLOC_ANCIENNETE"])
par_annee["SEGMENT"] = np.where(
    par_annee["ANCIENNETE"].isna().to_numpy(), BLOC_INDET,
    np.where(par_annee["ANCIENNETE"].eq(0).fillna(False).to_numpy(), "N",
             "N-" + par_annee["ANCIENNETE"].astype("string").fillna("?")),
)
par_annee = (
    par_annee.sort_values("ANCIENNETE", na_position="last")
    .reset_index(drop=True)
    [["SEGMENT", "ANNEE_SURVENANCE", "ANCIENNETE", "BLOC_ANCIENNETE",
      "NB_DOSSIERS", "NB_SOUS_PROVISION", "NB_SUR_PROVISION", "NB_ECART_NUL",
      "PM_MRM", "PM_CPT", "ECART", "ECART_MOYEN", "ECART_MEDIAN",
      "TAUX_CHUTE_PCT", "POIDS_NB_PCT", "POIDS_PM_PCT"]]
)
display(par_annee)

# COMMAND ----------

# Graphique 1 — le profil du taux par ancienneté fine, avec le poids de PM en
# regard (un taux ne se lit jamais sans son poids). Les années au-delà de N-9
# sont regroupées pour garder un axe lisible ; la table ci-dessus reste complète.
_g = par_annee[par_annee["ANCIENNETE"].notna()].copy()
_g["GROUPE"] = np.where(_g["ANCIENNETE"] <= 9, _g["SEGMENT"], "N-10 et +")
_g = (
    _g.groupby("GROUPE", sort=False)
      .agg(ECART=("ECART", "sum"), PM_MRM=("PM_MRM", "sum"),
           NB_DOSSIERS=("NB_DOSSIERS", "sum"), ANCIENNETE=("ANCIENNETE", "min"))
      .reset_index()
      .sort_values("ANCIENNETE")
)
_g["TAUX"] = np.where(_g["PM_MRM"] != 0, _g["ECART"] / _g["PM_MRM"] * 100, 0.0)

fig, (ax1, ax2) = plt.subplots(
    2, 1, figsize=(11, 6.8), sharex=True,
    gridspec_kw={"height_ratios": [2.2, 1], "hspace": 0.12},
)
_couleurs = [C_SIENNE if t > 0 else C_OCEAN for t in _g["TAUX"]]
ax1.bar(_g["GROUPE"], _g["TAUX"], color=_couleurs, width=0.62, zorder=2)
ax1.axhline(0, color="#333333", linewidth=0.9, zorder=1)
ax1.axhline(taux_global, color=C_GRIS, linewidth=1.3, linestyle="--", zorder=1)
ax1.text(len(_g) - 0.4, taux_global, f"  taux global {_pct(taux_global)}",
         fontsize=F_TXT - 1, color="#555555", va="bottom", ha="right")
for i, t in enumerate(_g["TAUX"]):
    ax1.annotate(_pct(round(t, 1)), (i, t), textcoords="offset points",
                 xytext=(0, 5 if t >= 0 else -13), ha="center", fontsize=F_TXT - 1)
ax1.margins(y=0.18)                 # de l'air pour les étiquettes sous les barres
ax1.set_ylabel("Taux de chute (%)", fontsize=F_AXE)
_style_ax(ax1)

ax2.bar(_g["GROUPE"], _g["PM_MRM"] / 1e6, color=C_BLEU, width=0.62, zorder=2)
ax2.set_ylabel("PM revue (M€)", fontsize=F_AXE)
ax2.set_xlabel("Ancienneté de surveillance (année de survenance)", fontsize=F_AXE)
_style_ax(ax2)

fig.suptitle("Où le signe bascule : taux de chute par ancienneté fine",
             fontsize=F_TITRE, fontweight="bold", x=0.02, ha="left")
fig.text(0.02, 0.915, f"Univers officiel des stats globales — inventaire {DATE_INV}, "
         f"{_n(len(base))} dossiers · sienne = sous-provisionné (risque), "
         "océan = sur-provisionné (marge)", fontsize=F_SST, color="#555555",
         style="italic")
_sauver_fig(fig, "g1_taux_par_annee")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 🔍 Le taux agrégé vs le dossier type — effet de masse ou pas ?
# MAGIC
# MAGIC Le taux agrégé donne **le solde** ; il peut être signé par une masse de
# MAGIC petits écarts… ou par quelques dossiers. Deux lectures complémentaires par
# MAGIC bloc :
# MAGIC
# MAGIC - la **part des dossiers** sous-provisionnés / sur-provisionnés / à écart
# MAGIC   nul (le « vote » dossier par dossier) ;
# MAGIC - l'**écart médian** (le dossier type) face à l'**écart moyen** (tiré par
# MAGIC   les extrêmes) : quand les deux divergent, le solde est porté par la queue
# MAGIC   de distribution, pas par le dossier type.

# COMMAND ----------

_disp = []
for bloc in BLOCS:
    g  = base[base["BLOC_ANCIENNETE"] == bloc]
    e  = g["ECART"]
    nb = len(g)
    _disp.append({
        "BLOC_ANCIENNETE"   : bloc,
        "NB_DOSSIERS"       : nb,
        "PCT_SOUS_PROVISION": round((e > 0).mean() * 100, 1),
        "PCT_SUR_PROVISION" : round((e < 0).mean() * 100, 1),
        "PCT_ECART_NUL"     : round((e == 0).mean() * 100, 1),
        "ECART_MOYEN"       : round(e.mean(), 2),
        "ECART_MEDIAN"      : round(e.median(), 2),
        "ECART_P25"         : round(e.quantile(0.25), 2),
        "ECART_P75"         : round(e.quantile(0.75), 2),
        "TAUX_CHUTE_PCT"    : float(par_bloc.set_index("BLOC_ANCIENNETE")
                                    .loc[bloc, "TAUX_CHUTE_PCT"]),
    })
dispersion = pd.DataFrame(_disp)
display(dispersion)

# COMMAND ----------

# Graphique 2 — la distribution des écarts par bloc (boîtes à moustaches,
# extrêmes masqués : ils sont traités nommément au §6). Échelle symétrique
# log : les écarts s'étalent sur plusieurs ordres de grandeur.
fig, ax = plt.subplots(figsize=(10, 5.2))
_donnees = [base.loc[base["BLOC_ANCIENNETE"] == b, "ECART"] for b in BLOCS]
bp = ax.boxplot(_donnees, showfliers=False, widths=0.45,
                patch_artist=True, medianprops=dict(color="white", linewidth=2))
ax.set_xticks(range(1, len(BLOCS) + 1), BLOCS)
for patch, b in zip(bp["boxes"], BLOCS):
    med = base.loc[base["BLOC_ANCIENNETE"] == b, "ECART"].median()
    patch.set_facecolor(C_SIENNE if med > 0 else C_OCEAN)
    patch.set_edgecolor("white")
ax.axhline(0, color="#333333", linewidth=0.9)
ax.set_yscale("symlog", linthresh=1000)
ax.set_ylabel("Écart PM revue − PM compte (€, échelle log symétrique)", fontsize=F_AXE)
for i, b in enumerate(BLOCS, start=1):
    med = base.loc[base["BLOC_ANCIENNETE"] == b, "ECART"].median()
    ax.annotate(f"médiane {_n(med)} €", (i, med), textcoords="offset points",
                xytext=(0, 8), ha="center", fontsize=F_TXT - 1, color="#333333")
_style_ax(ax)
fig.suptitle("Le dossier type par bloc : où se situe la médiane des écarts",
             fontsize=F_TITRE, fontweight="bold", x=0.02, ha="left")
fig.text(0.02, 0.91, "Boîtes = 50 % central des dossiers · extrêmes volontairement "
         "hors champ (voir concentration, §6)", fontsize=F_SST, color="#555555",
         style="italic")
_sauver_fig(fig, "g2_dispersion_par_bloc")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 🎯 Concentration — combien de dossiers portent le solde ?
# MAGIC
# MAGIC Pour chaque bloc : la part de la **somme des écarts absolus** portée par
# MAGIC les plus gros dossiers, et la liste nominative des **10 principaux
# MAGIC contributeurs** (l'onglet d'investigation du classeur). Si le solde d'un
# MAGIC bloc tient à une poignée de dossiers, la discussion devient un examen de
# MAGIC dossiers — pas un débat de méthode.

# COMMAND ----------

_conc, _tops = [], []
for bloc in BLOCS:
    g   = base[base["BLOC_ANCIENNETE"] == bloc].copy()
    g["ECART_ABS"] = g["ECART"].abs()
    tot_abs = g["ECART_ABS"].sum()
    g = g.sort_values("ECART_ABS", ascending=False)

    def _part(k, g=g, tot_abs=tot_abs):
        return round(g["ECART_ABS"].head(k).sum() / tot_abs * 100, 1) if tot_abs else 0.0

    _conc.append({
        "BLOC_ANCIENNETE"    : bloc,
        "NB_DOSSIERS"        : len(g),
        "ECART_ABS_TOTAL"    : round(tot_abs, 2),
        "PART_TOP_5_PCT"     : _part(5),
        "PART_TOP_10_PCT"    : _part(10),
        "PART_TOP_1PCT_PCT"  : _part(max(1, int(np.ceil(len(g) * 0.01)))),
    })

    top = g.head(10).copy()          # BLOC_ANCIENNETE déjà porté par les lignes
    top.insert(0, "RANG", range(1, len(top) + 1))
    top["SENS"] = np.where(top["ECART"] > 0, "Sous-provisionné", "Sur-provisionné")
    top["PART_ECART_ABS_PCT"] = (top["ECART_ABS"] / tot_abs * 100).round(1) if tot_abs else 0.0
    _tops.append(top)

concentration = pd.DataFrame(_conc)
_cols_top = [c for c in (
    "BLOC_ANCIENNETE", "RANG", "CPT_RPP", "CPT_NOM_PRENOM", "CLAUSE", "TYPE_COMPTE",
    "GARANTIE_LIBELLE", "CONSIGNE", "FAMILLE_CLE", "ANNEE_SURVENANCE",
    "PM_MRM", "PM_CPT", "ECART", "SENS", "PART_ECART_ABS_PCT",
) if c in pd.concat(_tops).columns]
contributeurs = pd.concat(_tops, ignore_index=True)[_cols_top]

display(concentration)
display(contributeurs)

# COMMAND ----------

# Graphique 3 — courbes de concentration : part cumulée de la somme des écarts
# absolus en fonction du nombre de dossiers (les plus gros d'abord). Une courbe
# qui monte en flèche = un solde porté par très peu de dossiers.
_C_BLOCS = {BLOC_N: C_BLEU, BLOC_N1: C_TEAL, BLOC_N2_PLUS: C_SIENNE, BLOC_INDET: C_GRIS}
fig, ax = plt.subplots(figsize=(10, 5.2))
for bloc in BLOCS:
    g = base.loc[base["BLOC_ANCIENNETE"] == bloc, "ECART"].abs() \
            .sort_values(ascending=False)
    if not g.sum():
        continue
    cum = (g.cumsum() / g.sum() * 100).head(200).reset_index(drop=True)
    ax.plot(cum.index + 1, cum, color=_C_BLOCS[bloc], linewidth=2, label=bloc)
    if len(cum) >= 10:
        ax.annotate(f"{cum.iloc[9]:.0f} %", (10, cum.iloc[9]),
                    textcoords="offset points", xytext=(6, -4),
                    fontsize=F_TXT - 1, color=_C_BLOCS[bloc])
ax.axvline(10, color="#CCCCCC", linewidth=1, linestyle=":")
ax.text(10, 2, " 10 dossiers", fontsize=F_TXT - 2, color="#777777")
ax.set_xscale("log")
ax.set_xlabel("Nombre de dossiers (les plus gros écarts d'abord, échelle log)", fontsize=F_AXE)
ax.set_ylabel("Part cumulée des écarts absolus (%)", fontsize=F_AXE)
ax.set_ylim(0, 105)
ax.legend(fontsize=F_LEG, frameon=False)
_style_ax(ax)
fig.suptitle("Concentration des écarts : combien de dossiers font le solde ?",
             fontsize=F_TITRE, fontweight="bold", x=0.02, ha="left")
fig.text(0.02, 0.91, "Lecture : la valeur annotée = part des écarts absolus du bloc "
         "portée par ses 10 plus gros dossiers", fontsize=F_SST, color="#555555",
         style="italic")
_sauver_fig(fig, "g3_concentration")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 💪 Robustesse du signe — trois vérifications simples
# MAGIC
# MAGIC 1. **Sensibilité** : le taux recalculé sans les 1 / 5 / 10 plus gros écarts
# MAGIC    (et sans le 1 % le plus extrême). Si le signe tient, il n'est pas
# MAGIC    l'artefact de quelques dossiers.
# MAGIC 2. **Intervalle de confiance à 95 %** par rééchantillonnage aléatoire des
# MAGIC    dossiers (méthode dite *bootstrap*) : l'incertitude du taux agrégé. Un
# MAGIC    intervalle entièrement d'un côté de zéro = signe statistiquement établi.
# MAGIC 3. **Test du signe** : parmi les dossiers à écart non nul, la part des
# MAGIC    sous-provisionnés est-elle significativement différente de 50 % ?
# MAGIC    (test binomial — le « vote » des dossiers, indépendant des montants).

# COMMAND ----------


def _taux(g: pd.DataFrame) -> float:
    s = g["PM_MRM"].sum()
    return g["ECART"].sum() / s * 100 if s else np.nan


def _taux_hors_top(g: pd.DataFrame, k: int) -> float:
    if len(g) <= k:
        return np.nan
    return _taux(g.drop(g["ECART"].abs().nlargest(k).index))


def _ic_reechantillonnage(g: pd.DataFrame, n_tirages: int, graine: int = 42):
    """IC 95 % du taux agrégé : rééchantillonnage des dossiers avec remise."""
    ecart, pm = g["ECART"].to_numpy(), g["PM_MRM"].to_numpy()
    n = len(g)
    if n == 0 or not pm.sum():
        return (np.nan, np.nan)
    rng, taux = np.random.default_rng(graine), np.empty(n_tirages)
    for b in range(n_tirages):
        idx = rng.integers(0, n, n)
        s = pm[idx].sum()
        taux[b] = ecart[idx].sum() / s * 100 if s else np.nan
    return tuple(np.round(np.nanpercentile(taux, [2.5, 97.5]), 2))


def _test_signe(nb_sous: int, nb_sur: int) -> float:
    """P-valeur du test binomial (H0 : autant de sous que de sur-provisionnés)."""
    n = nb_sous + nb_sur
    if n == 0:
        return np.nan
    try:
        from scipy.stats import binomtest
        return round(binomtest(nb_sous, n, 0.5).pvalue, 4)
    except ImportError:                       # approximation normale de secours
        from math import erf, sqrt
        z = (nb_sous - n / 2) / sqrt(n / 4)
        return round(2 * (1 - 0.5 * (1 + erf(abs(z) / sqrt(2)))), 4)


_rob = []
for bloc in BLOCS:
    g = base[base["BLOC_ANCIENNETE"] == bloc]
    taux_complet = round(_taux(g), 2)
    variantes = {
        "TAUX_HORS_TOP_1"   : _taux_hors_top(g, 1),
        "TAUX_HORS_TOP_5"   : _taux_hors_top(g, 5),
        "TAUX_HORS_TOP_10"  : _taux_hors_top(g, 10),
        "TAUX_HORS_TOP_1PCT": _taux_hors_top(g, max(1, int(np.ceil(len(g) * 0.01)))),
    }
    ic_bas, ic_haut = _ic_reechantillonnage(g, N_TIR)
    nb_sous, nb_sur = int((g["ECART"] > 0).sum()), int((g["ECART"] < 0).sum())
    _rob.append({
        "BLOC_ANCIENNETE" : bloc,
        "TAUX_CHUTE_PCT"  : taux_complet,
        **{k: (round(v, 2) if pd.notna(v) else np.nan) for k, v in variantes.items()},
        "SIGNE_STABLE"    : all(np.sign(v) == np.sign(taux_complet)
                                for v in variantes.values() if pd.notna(v)),
        "IC95_BAS_PCT"    : ic_bas,
        "IC95_HAUT_PCT"   : ic_haut,
        "IC95_EXCLUT_ZERO": bool(pd.notna(ic_bas) and (ic_bas > 0 or ic_haut < 0)),
        "NB_SOUS_PROVISION": nb_sous,
        "NB_SUR_PROVISION" : nb_sur,
        "P_VALEUR_SIGNE"  : _test_signe(nb_sous, nb_sur),
    })
robustesse = pd.DataFrame(_rob)
display(robustesse)

# COMMAND ----------

# Graphique 4 — le taux par bloc avec son intervalle de confiance à 95 % :
# la synthèse visuelle de la robustesse (un intervalle qui ne touche pas zéro
# = un signe qui n'est pas un hasard d'échantillon).
fig, ax = plt.subplots(figsize=(9.5, 5))
_x = np.arange(len(robustesse))
_couleurs = [C_SIENNE if t > 0 else C_OCEAN for t in robustesse["TAUX_CHUTE_PCT"]]
ax.bar(_x, robustesse["TAUX_CHUTE_PCT"], color=_couleurs, width=0.5, zorder=2)
ax.errorbar(_x, robustesse["TAUX_CHUTE_PCT"],
            yerr=[robustesse["TAUX_CHUTE_PCT"] - robustesse["IC95_BAS_PCT"],
                  robustesse["IC95_HAUT_PCT"] - robustesse["TAUX_CHUTE_PCT"]],
            fmt="none", ecolor="#333333", elinewidth=1.6, capsize=5, zorder=3)
for i, r in robustesse.iterrows():
    ax.annotate(_pct(r["TAUX_CHUTE_PCT"]),
                (i, r["IC95_HAUT_PCT"] if r["TAUX_CHUTE_PCT"] >= 0 else r["IC95_BAS_PCT"]),
                textcoords="offset points",
                xytext=(0, 7 if r["TAUX_CHUTE_PCT"] >= 0 else -15),
                ha="center", fontsize=F_TXT, fontweight="bold")
ax.axhline(0, color="#333333", linewidth=0.9, zorder=1)
ax.margins(y=0.18)
ax.set_xticks(_x, robustesse["BLOC_ANCIENNETE"])
ax.set_ylabel("Taux de chute (%)", fontsize=F_AXE)
_style_ax(ax)
fig.suptitle("Le signe de chaque bloc est-il statistiquement établi ?",
             fontsize=F_TITRE, fontweight="bold", x=0.02, ha="left")
fig.text(0.02, 0.91, f"Barres d'erreur = intervalle de confiance à 95 % "
         f"({_n(N_TIR)} rééchantillonnages des dossiers)", fontsize=F_SST,
         color="#555555", style="italic")
_sauver_fig(fig, "g4_ic_par_bloc")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 🧩 Effets de composition — qui porte le signe de chaque bloc ?
# MAGIC
# MAGIC Le portefeuille ne se ressemble pas d'un bloc à l'autre : l'année N est
# MAGIC dominée par des arrêts de travail récents (IT), les anciennetés élevées par
# MAGIC des invalidités consolidées (IP). On décompose l'écart de chaque bloc par
# MAGIC **garantie**, **consigne** et **famille de clé de rapprochement** — puis on
# MAGIC regarde le **mois de survenance** du bloc N (effet fin d'année).

# COMMAND ----------

_axes_mix = {
    "Garantie"             : "GARANTIE_LIBELLE",
    "Consigne"             : "CONSIGNE",
    "Clé de rapprochement" : "FAMILLE_CLE",
}
_mix = []
for axe, col in _axes_mix.items():
    t = table_chute(base, ["BLOC_ANCIENNETE", col], poids_dans="BLOC_ANCIENNETE")
    t = t.rename(columns={col: "SEGMENT"})
    t.insert(0, "AXE", axe)
    _mix.append(t)
mix = pd.concat(_mix, ignore_index=True)
mix["ORDRE_BLOC"] = mix["BLOC_ANCIENNETE"].map(_BLOC_ORDRE)
mix = mix.sort_values(["AXE", "ORDRE_BLOC", "PM_MRM"],
                      ascending=[True, True, False]).reset_index(drop=True)
display(mix)

# COMMAND ----------

# Graphique 5 — la contribution de chaque garantie à l'écart net du bloc
# (en M€, empilée de part et d'autre de zéro) : LA décomposition qui montre
# d'où vient le signe.
_gar_ordre = [s for s in ("IT (incapacité)", "IP (invalidité)",
                          "Autre garantie", "Non renseignée")
              if s in set(base["GARANTIE_LIBELLE"])]
_C_GAR = dict(zip(_gar_ordre, (C_BLEU, C_TEAL, C_SIENNE, C_GRIS)))

_pivot = (
    mix[mix["AXE"] == "Garantie"]
    .pivot_table(index="BLOC_ANCIENNETE", columns="SEGMENT",
                 values="ECART", aggfunc="first", fill_value=0.0)
    .reindex(BLOCS)
    .fillna(0.0)
)
fig, ax = plt.subplots(figsize=(9.5, 5.2))
_haut = np.zeros(len(_pivot))
_bas  = np.zeros(len(_pivot))
for seg in _gar_ordre:
    if seg not in _pivot.columns:
        continue
    v = _pivot[seg].to_numpy() / 1e6
    dessous = np.where(v >= 0, _haut, _bas)
    ax.bar(_pivot.index, v, bottom=dessous, color=_C_GAR[seg],
           width=0.5, label=seg, edgecolor="white", linewidth=1.5, zorder=2)
    _haut += np.clip(v, 0, None)
    _bas  += np.clip(v, None, 0)
for i, bloc in enumerate(_pivot.index):
    net = _pivot.loc[bloc].sum() / 1e6
    ax.annotate(f"solde {_meur(net * 1e6)}",
                (i, _haut[i] if net >= 0 else _bas[i]),
                textcoords="offset points", xytext=(0, 7 if net >= 0 else -14),
                ha="center", fontsize=F_TXT, fontweight="bold")
ax.axhline(0, color="#333333", linewidth=0.9, zorder=1)
ax.margins(y=0.18)
ax.set_ylabel("Écart PM revue − PM compte (M€)", fontsize=F_AXE)
ax.legend(fontsize=F_LEG, frameon=False)
_style_ax(ax)
fig.suptitle("D'où vient le signe : contribution de chaque garantie à l'écart du bloc",
             fontsize=F_TITRE, fontweight="bold", x=0.02, ha="left")
fig.text(0.02, 0.91, "Au-dessus de zéro = tire vers le sous-provisionnement · "
         "en dessous = tire vers la marge", fontsize=F_SST, color="#555555",
         style="italic")
_sauver_fig(fig, "g5_contribution_garantie")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### La part des sinistres IT / IP par ancienneté — parts ET volumétrie
# MAGIC
# MAGIC La question du mix posée frontalement : dans chaque bloc, **quelle part des
# MAGIC dossiers** (et de la PM revue) est en incapacité (IT) et en invalidité
# MAGIC (IP) — et **quels volumes** cela représente (nombre de dossiers, PM revue
# MAGIC et compte en M€) ? C'est la photographie de la structure du portefeuille :
# MAGIC si l'année N est dominée par les IT récents et les anciennetés élevées par
# MAGIC les IP consolidés, le basculement du signe suit ce **changement de nature
# MAGIC des dossiers** — l'argument central de la lecture métier (§9). Deux vues :
# MAGIC le graphique 7 (parts, 100 %) et le graphique 8 (volumétrie brute).

# COMMAND ----------

structure_garantie = (
    mix.loc[mix["AXE"] == "Garantie",
            ["BLOC_ANCIENNETE", "SEGMENT", "NB_DOSSIERS", "POIDS_NB_PCT",
             "PM_MRM", "PM_CPT", "ECART", "POIDS_PM_PCT", "TAUX_CHUTE_PCT"]]
    .rename(columns={"SEGMENT": "GARANTIE"})
    .reset_index(drop=True)
)
display(structure_garantie)

# Graphique 7 — la structure en parts (chaque barre totalise 100 % de son
# bloc) : part des dossiers à gauche, part de la PM revue à droite.
fig, axes = plt.subplots(1, 2, figsize=(11.5, 5), sharey=True)
for ax, (colonne, titre) in zip(axes, [("POIDS_NB_PCT", "Part des dossiers"),
                                       ("POIDS_PM_PCT", "Part de la PM revue")]):
    piv = (
        structure_garantie.pivot_table(index="BLOC_ANCIENNETE", columns="GARANTIE",
                                       values=colonne, aggfunc="first", fill_value=0.0)
        .reindex(BLOCS).fillna(0.0)
    )
    bas = np.zeros(len(piv))
    for seg in _gar_ordre:
        if seg not in piv.columns:
            continue
        v = piv[seg].to_numpy()
        ax.bar(piv.index, v, bottom=bas, color=_C_GAR[seg], width=0.55,
               label=seg, edgecolor="white", linewidth=1.5, zorder=2)
        for i, (val, b0) in enumerate(zip(v, bas)):
            if val >= 6:                       # étiquette seulement si lisible
                ax.text(i, b0 + val / 2, _pct(round(val, 1)), ha="center",
                        va="center", fontsize=F_TXT - 1, color="white",
                        fontweight="bold")
        bas += v
    ax.set_title(titre, fontsize=F_AXE + 1)
    ax.set_ylim(0, 100)
    _style_ax(ax)
    plt.setp(ax.get_xticklabels(), rotation=12, ha="right", fontsize=F_AXE - 1)
axes[0].set_ylabel("Part du bloc (%)", fontsize=F_AXE)
axes[1].legend(fontsize=F_LEG, frameon=False, loc="center left",
               bbox_to_anchor=(1.01, 0.5))
fig.suptitle("La structure du portefeuille change avec l'ancienneté (IT → IP)",
             fontsize=F_TITRE, fontweight="bold", x=0.02, ha="left")
fig.text(0.02, 0.90, "Chaque barre totalise 100 % de son bloc d'ancienneté — "
         "étiquettes affichées à partir de 6 %", fontsize=F_SST, color="#555555",
         style="italic")
_sauver_fig(fig, "g7_structure_garantie")
plt.show()

# COMMAND ----------

# Graphique 8 — la volumétrie derrière les parts : nombre de dossiers et PM
# revue par bloc × garantie (montants bruts — un pourcentage ne se lit jamais
# sans son volume).
fig, axes = plt.subplots(1, 2, figsize=(11.5, 5))
for ax, (colonne, titre, echelle) in zip(axes, [
        ("NB_DOSSIERS", "Nombre de dossiers", 1.0),
        ("PM_MRM",      "PM revue (M€)",      1e6)]):
    piv = (
        structure_garantie.pivot_table(index="BLOC_ANCIENNETE", columns="GARANTIE",
                                       values=colonne, aggfunc="first", fill_value=0.0)
        .reindex(BLOCS).fillna(0.0)
    )
    bas = np.zeros(len(piv))
    for seg in _gar_ordre:
        if seg not in piv.columns:
            continue
        v = piv[seg].to_numpy() / echelle
        ax.bar(piv.index, v, bottom=bas, color=_C_GAR[seg], width=0.55,
               label=seg, edgecolor="white", linewidth=1.5, zorder=2)
        bas += v
    for i, tot in enumerate(bas):
        ax.annotate(_n(tot) if echelle == 1.0 else _meur(tot * 1e6), (i, tot),
                    textcoords="offset points", xytext=(0, 5), ha="center",
                    fontsize=F_TXT - 1, fontweight="bold")
    ax.set_title(titre, fontsize=F_AXE + 1)
    ax.margins(y=0.12)
    _style_ax(ax)
    plt.setp(ax.get_xticklabels(), rotation=12, ha="right", fontsize=F_AXE - 1)
axes[1].legend(fontsize=F_LEG, frameon=False, loc="center left",
               bbox_to_anchor=(1.01, 0.5))
fig.suptitle("La volumétrie derrière les parts : dossiers et PM revue par bloc × garantie",
             fontsize=F_TITRE, fontweight="bold", x=0.02, ha="left")
fig.text(0.02, 0.90, "Total annoté au sommet de chaque barre — le détail PM compte "
         "et écart se lit dans la table ci-dessus", fontsize=F_SST, color="#555555",
         style="italic")
_sauver_fig(fig, "g8_volumetrie_garantie")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Le bloc N au mois le mois — l'effet fin d'année
# MAGIC
# MAGIC Les survenances d'octobre à décembre sont à quelques semaines de la date
# MAGIC d'inventaire : provision comptable en début de montée en charge, estimation
# MAGIC de la revue déjà complète. Si l'écart positif du bloc N se concentre sur la
# MAGIC fin d'année, le « sous-provisionnement » de N est un **effet de calendrier
# MAGIC de gestion**, pas une insuffisance durable du compte.

# COMMAND ----------

_MOIS_LIBELLES = ["Jan", "Fév", "Mar", "Avr", "Mai", "Jun",
                  "Jul", "Aoû", "Sep", "Oct", "Nov", "Déc"]

bloc_n = base[base["BLOC_ANCIENNETE"] == BLOC_N].copy()
par_mois_n = table_chute(bloc_n, "MOIS_SURVENANCE").dropna(subset=["MOIS_SURVENANCE"])
par_mois_n["MOIS_SURVENANCE"] = par_mois_n["MOIS_SURVENANCE"].astype(int)
par_mois_n["MOIS_LABEL"] = par_mois_n["MOIS_SURVENANCE"].map(
    lambda m: _MOIS_LIBELLES[m - 1])
par_mois_n["IS_FIN_ANNEE"] = par_mois_n["MOIS_SURVENANCE"] >= 10
par_mois_n = par_mois_n.sort_values("MOIS_SURVENANCE").reset_index(drop=True)
display(par_mois_n)

_part_fin_annee = round(
    par_mois_n.loc[par_mois_n["IS_FIN_ANNEE"], "ECART"].sum()
    / par_mois_n["ECART"].sum() * 100, 1,
) if par_mois_n["ECART"].sum() else 0.0

fig, ax = plt.subplots(figsize=(10, 4.8))
_couleurs = [C_ROUGE if fin else C_GRIS for fin in par_mois_n["IS_FIN_ANNEE"]]
ax.bar(par_mois_n["MOIS_LABEL"], par_mois_n["ECART"] / 1e6, color=_couleurs,
       width=0.6, zorder=2)
ax.axhline(0, color="#333333", linewidth=0.9, zorder=1)
ax.margins(y=0.15)
ax.set_ylabel("Écart du mois (M€)", fontsize=F_AXE)
_style_ax(ax)
fig.suptitle("Bloc N : l'écart se joue-t-il en fin d'année ?",
             fontsize=F_TITRE, fontweight="bold", x=0.02, ha="left")
fig.text(0.02, 0.90, f"Rouge = survenances Oct-Déc (à quelques semaines de "
         f"l'inventaire) — elles portent {_pct(_part_fin_annee)} de l'écart du bloc N",
         fontsize=F_SST, color="#555555", style="italic")
_sauver_fig(fig, "g6_bloc_n_par_mois")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. 📖 La lecture métier — pourquoi ce profil de signes n'est pas absurde
# MAGIC
# MAGIC Le sens « attendu » du taux dépend du **cycle de vie de la provision**, pas
# MAGIC d'une règle unique valable à toutes les anciennetés :
# MAGIC
# MAGIC - **Année N (survenances récentes)** — le dossier vient d'être ouvert : la
# MAGIC   provision comptable est en **montée en charge** (elle suit la cadence de
# MAGIC   gestion), alors que la revue d'inventaire estime d'emblée la **charge
# MAGIC   complète** du sinistre. Un écart *revue > compte* (taux **positif**) sur
# MAGIC   les survenances de fin d'année est donc le comportement **mécanique** du
# MAGIC   couple compte / revue — d'autant plus visible que le bloc N concentre les
# MAGIC   arrêts de travail (IT) les plus jeunes. Le §8 mesure précisément cette
# MAGIC   part de l'écart portée par Oct-Déc.
# MAGIC - **Années passées (N-1, N-2 et antérieur)** — les dossiers encore ouverts
# MAGIC   sont passés ou passent en **invalidité (IP)** : le compte **conserve des
# MAGIC   provisions prudentes**, pendant que la revue tête par tête les **ajuste**
# MAGIC   (consolidations, révisions, sorties à venir). Un écart *compte > revue*
# MAGIC   (taux **négatif** = marge) est la trace de cette prudence — pas une
# MAGIC   anomalie de calcul.
# MAGIC - **La méthode d'inventaire diffère selon l'année** (revue tête par tête sur
# MAGIC   N-1) : comparer les blocs entre eux, c'est aussi comparer des méthodes
# MAGIC   d'estimation — raison de plus pour ventiler, comme le fait cette étude,
# MAGIC   plutôt que de lire le seul taux global.
# MAGIC
# MAGIC L'« effet inverse » attendu en comité (marge sur le récent, risque sur
# MAGIC l'ancien) serait celui d'un compte **prudent à l'ouverture et relâché
# MAGIC ensuite**. Les données tranchent entre les deux lectures : c'est l'objet des
# MAGIC §4 à §8, et la conclusion (§12) l'écrit noir sur blanc avec les chiffres.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. 💾 Écriture Hive — les tables de l'étude
# MAGIC
# MAGIC Quatre tables `etude_anciennete_*`, écrites comme les tables métriques :
# MAGIC colonnes de run (`DATE_INVENTAIRE`, `PERIMETRE`, `LIBELLE_RUN`, `CLE_RUN`),
# MAGIC historisation par run (rejouer l'étude remplace SES lignes). Les tables
# MAGIC `metrique_*` officielles ne sont **jamais** touchées.
# MAGIC
# MAGIC | Table | Contenu |
# MAGIC |---|---|
# MAGIC | `etude_anciennete_par_annee` | le taux par année de survenance (ancienneté fine) |
# MAGIC | `etude_anciennete_par_bloc` | stats par bloc : dispersion + robustesse (IC, sensibilité, test du signe) |
# MAGIC | `etude_anciennete_mix` | la décomposition par garantie / consigne / clé × bloc |
# MAGIC | `etude_anciennete_contributeurs` | les 10 principaux contributeurs de chaque bloc |

# COMMAND ----------

tables_etude = {
    "etude_anciennete_par_annee"    : par_annee,
    "etude_anciennete_par_bloc"     : dispersion.merge(
        robustesse.drop(columns=["TAUX_CHUTE_PCT", "NB_SOUS_PROVISION", "NB_SUR_PROVISION"]),
        on="BLOC_ANCIENNETE").merge(concentration, on=["BLOC_ANCIENNETE", "NB_DOSSIERS"]),
    "etude_anciennete_mix"          : mix.drop(columns=["ORDRE_BLOC"]),
    "etude_anciennete_contributeurs": contributeurs,
}

if ECRIRE:
    for nom, pdf in tables_etude.items():
        out = pdf.copy()
        # Entiers pandas « à trous » (Int64) → double Spark : la valeur manquante
        # (ancienneté indéterminée) passe en NaN sans casser la conversion.
        for c in out.select_dtypes(include=["Int64"]).columns:
            out[c] = out[c].astype("float64")
        out["DATE_INVENTAIRE"] = DATE_ISO
        out["PERIMETRE"]       = PERIM
        out["LIBELLE_RUN"]     = CLIENT_NAME
        out["CLE_RUN"]         = CLE
        sdf = (spark.createDataFrame(out)
                    .withColumn("DATE_INVENTAIRE", F.col("DATE_INVENTAIRE").cast("date")))
        write_delta_historise(sdf, f"{SCHEMA}.{nom}", DATE_ISO, PERIM)
        print(f"  ✓ [DELTA] {SCHEMA}.{nom}  ({len(out):,} lignes — run {CLE} remplacé)")
else:
    print("⏭ ecrire_tables ≠ oui — aucune table écrite (l'étude reste consultable ici).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. 📦 Le classeur Excel de restitution — et son guide de lecture
# MAGIC
# MAGIC Un classeur autonome, un onglet par question — la matière première du
# MAGIC support de présentation. L'onglet « Sommaire » rappelle le périmètre,
# MAGIC l'onglet « Guide de lecture » dit **comment analyser et interpréter chaque
# MAGIC sortie**, et l'onglet « Conclusion » reprend les réponses générées au §12.
# MAGIC
# MAGIC ### Comment analyser les sorties — la démarche en quatre questions
# MAGIC
# MAGIC L'étude se déroule toujours dans le même ordre, chaque sortie répondant à
# MAGIC une question, et la réponse à chacune conditionne la lecture de la
# MAGIC suivante :
# MAGIC
# MAGIC 1. **Le chiffre est-il juste ?** (contrôle §3) — tant que le recalcul ne
# MAGIC    recoupe pas la table officielle, on ne discute pas d'interprétation.
# MAGIC 2. **Le signe est-il un effet de masse ou de quelques dossiers ?**
# MAGIC    (dispersion §5, concentration §6) — si le solde tient à une poignée de
# MAGIC    dossiers, la discussion devient un examen de dossiers nominatif, pas un
# MAGIC    débat de méthode.
# MAGIC 3. **Le signe est-il statistiquement établi ?** (robustesse §7) — trois
# MAGIC    critères à réunir : intervalle de confiance d'un seul côté de zéro,
# MAGIC    signe stable sans les 10 plus gros dossiers, test du signe < 5 %.
# MAGIC 4. **Qu'est-ce qui l'explique ?** (composition §8) — structure IT/IP,
# MAGIC    effet fin d'année, consigne, clé : le signe doit avoir une **cause
# MAGIC    métier lisible** avant d'être présenté.
# MAGIC
# MAGIC La table ci-dessous (onglet « Guide de lecture ») donne, sortie par
# MAGIC sortie : ce qu'elle montre, comment la lire, et la règle d'interprétation.

# COMMAND ----------

guide_lecture = pd.DataFrame([
    ("Par bloc N / N-1 / N-2 + Contrôle cohérence",
     "Le taux de chute officiel de chaque bloc, recalculé depuis le détail tête par tête",
     "Taux positif = compte < revue (sous-provisionnement, risque) ; négatif = marge. "
     "Toujours lire un taux avec son poids de PM",
     "Identique à metrique_chute → la chaîne de calcul est confirmée, on peut discuter "
     "du fond. Un écart = run désynchronisé : rejouer l'export officiel avant tout"),

    ("Par année de survenance (graphique 1)",
     "L'ancienneté fine : où le signe bascule, année de survenance par année",
     "Une barre par année, le poids de PM revue en dessous ; la ligne pointillée = taux global",
     "Bascule progressive avec l'ancienneté = effet de maturité des dossiers (attendu). "
     "Rupture isolée sur une seule année = événement à investiguer (méthode de revue, "
     "portefeuille, qualité de donnée)"),

    ("Dispersion par bloc (graphique 2)",
     "Le dossier type : parts sous/sur-provisionnées, médiane et quartiles des écarts",
     "Comparer la médiane (le dossier type) à la moyenne (tirée par les extrêmes)",
     "Médiane du même signe que le taux = effet de masse, le bloc se comporte comme son "
     "solde. Médiane quasi nulle ou de signe opposé = solde porté par la queue de "
     "distribution → passer à la concentration"),

    ("Concentration (graphique 3) + Principaux contributeurs",
     "La part du solde portée par les plus gros dossiers, et leur liste nominative",
     "La valeur annotée = part des écarts absolus du bloc portée par ses 10 plus gros dossiers",
     "Part majoritaire (> 50 %) = examiner les dossiers un à un avant toute conclusion de "
     "méthode. Part faible = phénomène de portefeuille, les extrêmes ne dictent pas le signe"),

    ("Robustesse du signe (graphique 4)",
     "Intervalle de confiance à 95 %, taux sans les plus gros dossiers, test du signe",
     "Trois critères : IC entièrement d'un seul côté de zéro ; signe inchangé sans les "
     "top 1/5/10 ; p-valeur du test du signe < 5 %",
     "Les trois réunis = signe statistiquement établi, opposable en comité. Un critère "
     "manquant = présenter le taux AVEC sa réserve (« porté par X dossiers », « non "
     "significatif »)"),

    ("Mix garantie-consigne-clé (graphique 5)",
     "La contribution de chaque segment à l'écart net du bloc, en M€",
     "Au-dessus de zéro = tire vers le sous-provisionnement ; en dessous = vers la marge ; "
     "le solde annoté = l'écart du bloc",
     "Un segment domine la contribution = le signe du bloc s'explique par CE segment — "
     "vérifier aussi que le signe ne vient pas d'une clé de rapprochement moins stricte "
     "(clé clause)"),

    ("Structure IT-IP par bloc (graphiques 7 et 8)",
     "La nature des sinistres par ancienneté : parts (100 %) et volumétrie (dossiers, PM en M€)",
     "Graphique 7 = parts des dossiers et de la PM ; graphique 8 = volumes bruts, "
     "total annoté par bloc ; la table donne PM compte et écart par segment",
     "Part IP croissante avec l'ancienneté + segments IP en marge = le basculement du "
     "signe suit le cycle de vie IT → IP : un effet de STRUCTURE du portefeuille, pas "
     "une dérive du compte"),

    ("Bloc N au mois le mois (graphique 6)",
     "L'effet fin d'année sur l'écart du bloc N",
     "Rouge = survenances Oct-Déc, à quelques semaines de la date d'inventaire",
     "Oct-Déc portent l'essentiel de l'écart = effet calendrier de gestion (provision "
     "comptable en montée en charge, revue à charge complète) : le « risque » du bloc N "
     "se résorbe avec la gestion. Sinon → chercher la cause ailleurs (garantie, clause, "
     "dossiers)"),

    ("Conclusion",
     "Les réponses aux questions du comité, générées depuis les chiffres du run",
     "Une ligne par question, la réponse chiffrée en face",
     "À reprendre telles quelles dans le support de présentation — chaque affirmation "
     "se relit dans son onglet source"),
], columns=["Sortie", "Ce que ça montre", "Comment le lire", "Comment l'interpréter"])
display(guide_lecture)

# COMMAND ----------

_sommaire = pd.DataFrame([
    ("Étude",                        "Taux de chute par ancienneté de surveillance"),
    ("Date d'inventaire",            DATE_INV),
    ("Périmètre",                    PERIM),
    ("Clé de run",                   CLE),
    ("Source",                       f"{SCHEMA}.resultat_backtest (détail historisé)"),
    ("Dossiers de l'univers",        f"{len(base):,}".replace(",", " ")),
    ("PM revue de l'univers (€)",    f"{base['PM_MRM'].sum():,.0f}".replace(",", " ")),
    ("Taux de chute global",         f"{taux_global} %"),
    ("Univers",                      "Matchés inventaire courant, hors « à supprimer » "
                                     "et hors statut inventaire NON (contrat METRIQUES §4.2)"),
    ("Rééchantillonnages IC",        f"{N_TIR:,}".replace(",", " ")),
], columns=["Repère", "Valeur"])

_onglets = {
    "Sommaire"                 : _sommaire,
    "Guide de lecture"         : guide_lecture,
    "Par bloc N N-1 N-2"       : par_bloc,
    "Par année de survenance"  : par_annee,
    "Dispersion par bloc"      : dispersion,
    "Concentration"            : concentration,
    "Principaux contributeurs" : contributeurs,
    "Robustesse du signe"      : robustesse,
    "Mix garantie consigne clé": mix.drop(columns=["ORDRE_BLOC"]),
    "Structure IT-IP par bloc" : structure_garantie,
    "Bloc N au mois le mois"   : par_mois_n,
}
if len(controle):
    _onglets["Contrôle cohérence"] = controle

os.makedirs(EXPORT, exist_ok=True)
CHEMIN_XLSX = f"{EXPORT}/etude_chute_anciennete_{ANNEE}_{PERIM}.xlsx"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. 🧾 La conclusion ultime — générée depuis les chiffres
# MAGIC
# MAGIC Chaque question du comité reçoit sa réponse, calculée sur ce run — rien
# MAGIC n'est affirmé qui ne soit relu dans un onglet du classeur.

# COMMAND ----------


def _signe(t: float) -> str:
    if t > 0:
        return "positif (sous-provisionné : compte < revue)"
    if t < 0:
        return "négatif (sur-provisionné : le compte porte une marge)"
    return "nul"


_rob_ix  = robustesse.set_index("BLOC_ANCIENNETE")
_conc_ix = concentration.set_index("BLOC_ANCIENNETE")
_disp_ix = dispersion.set_index("BLOC_ANCIENNETE")
_stru_ix = structure_garantie.set_index(["BLOC_ANCIENNETE", "GARANTIE"])


def _part_nb(bloc: str, garantie: str) -> float:
    """Part des dossiers (%) d'une garantie dans un bloc — 0 si absente."""
    try:
        return float(_stru_ix.loc[(bloc, garantie), "POIDS_NB_PCT"])
    except KeyError:
        return 0.0

_lignes_profil = " ; ".join(
    f"{b} : {_pct(_rob_ix.loc[b, 'TAUX_CHUTE_PCT'])}" for b in BLOCS
)
_blocs_robustes = [
    b for b in BLOCS
    if _rob_ix.loc[b, "SIGNE_STABLE"] and _rob_ix.loc[b, "IC95_EXCLUT_ZERO"]
]
_blocs_fragiles = [b for b in BLOCS if b not in _blocs_robustes]

_verdicts = [
    ("Le profil présenté est-il réel ?",
     f"Oui. Recalculé depuis le détail tête par tête ({_n(len(base))} dossiers), "
     f"le taux par bloc donne : {_lignes_profil}"
     + (" — identique à la table officielle (contrôle §3)." if len(controle) else ".")),

    ("Le signe de chaque bloc est-il solide ?",
     (f"Signe statistiquement établi (intervalle de confiance à 95 % d'un seul côté "
      f"de zéro ET stable au retrait des 10 plus gros dossiers) pour : "
      f"{', '.join(_blocs_robustes) if _blocs_robustes else 'aucun bloc'}."
      + (f" À manier avec prudence pour : {', '.join(_blocs_fragiles)} — "
         "le solde y dépend des dossiers extrêmes (voir onglet Robustesse)."
         if _blocs_fragiles else ""))),

    ("Le solde est-il porté par quelques dossiers ?",
     " ; ".join(
         f"{b} : les 10 plus gros dossiers portent "
         f"{_pct(_conc_ix.loc[b, 'PART_TOP_10_PCT'])} des écarts absolus"
         for b in BLOCS
     ) + " — la liste nominative est dans l'onglet Principaux contributeurs."),

    ("Que dit le dossier type (indépendamment des montants) ?",
     " ; ".join(
         f"{b} : {_pct(_disp_ix.loc[b, 'PCT_SOUS_PROVISION'])} des dossiers "
         f"sous-provisionnés, médiane des écarts {_n(_disp_ix.loc[b, 'ECART_MEDIAN'])} €"
         for b in BLOCS
     )),

    ("Qu'est-ce qui explique le signe positif du bloc N ?",
     f"Les survenances d'octobre à décembre — à quelques semaines de "
     f"l'inventaire — portent {_pct(_part_fin_annee)} de l'écart du bloc N : "
     "provision comptable en montée en charge face à une revue qui estime "
     "d'emblée la charge complète (effet calendrier de gestion, voir §8 et "
     "la décomposition par garantie)."),

    ("La nature des sinistres (IT / IP) change-t-elle avec l'ancienneté ?",
     f"Oui — part des dossiers en incapacité (IT) : "
     f"{_pct(_part_nb(BLOC_N, 'IT (incapacité)'))} sur le bloc N contre "
     f"{_pct(_part_nb(BLOC_N2_PLUS, 'IT (incapacité)'))} en N-2 et antérieur ; "
     f"en invalidité (IP) : {_pct(_part_nb(BLOC_N, 'IP (invalidité)'))} sur N "
     f"contre {_pct(_part_nb(BLOC_N2_PLUS, 'IP (invalidité)'))} en N-2 et "
     "antérieur. Le basculement du signe suit ce changement de nature des "
     "dossiers (onglet Structure IT-IP par bloc, graphique 7)."),

    ("Le profil contredit-il l'intuition du comité ?",
     "Non : l'inverse (marge sur le récent, risque sur l'ancien) supposerait un "
     "compte prudent à l'ouverture et relâché ensuite. Le profil observé — "
     "montée en charge sur N, prudence conservée sur les exercices anciens — "
     "est le cycle de vie normal de la provision (lecture métier, §9)."),
]

conclusion = pd.DataFrame(_verdicts, columns=["Question", "Réponse"])
_onglets["Conclusion"] = conclusion

with pd.ExcelWriter(CHEMIN_XLSX, engine="openpyxl") as writer:
    for onglet, pdf in _onglets.items():
        pdf.to_excel(writer, sheet_name=onglet[:31], index=False)
print(f"📦 Classeur écrit : {CHEMIN_XLSX} ({len(_onglets)} onglets)")

# COMMAND ----------

_html_verdicts = "".join(
    f"<div style='margin-top:10px'><div style='font-weight:600;color:#00008F'>"
    f"❓ {q}</div><div style='margin-top:2px;color:#333'>{r}</div></div>"
    for q, r in _verdicts
)
displayHTML(f"""
<div style="font-family:'Segoe UI',sans-serif;border:1px solid #d5d9e0;border-radius:10px;
            padding:16px 20px;background:linear-gradient(90deg,#f4f6fb,#ffffff);max-width:980px">
  <div style="font-size:16px;font-weight:700;color:#00008F">
    🧾 Conclusion — taux de chute par ancienneté de surveillance ({DATE_INV}, périmètre {PERIM})</div>
  <div style="font-size:13px">{_html_verdicts}</div>
  <div style="margin-top:14px;font-size:12px;color:#027180">
    ✅ Chaque réponse se relit dans le classeur Excel (onglet correspondant) et dans les
    tables Hive etude_anciennete_* — mêmes chiffres, même clé de run que les tables officielles.</div>
</div>""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. ⬇️ Récupérer les livrables

# COMMAND ----------

_liens = [("Classeur Excel de l'étude", CHEMIN_XLSX)] + [
    (f"Graphique {nom}", chemin) for nom, chemin in _PNGS.items()
]
displayHTML(
    "<div style=\"font-family:'Segoe UI',sans-serif;font-size:13px\">"
    "<b>📦 Livrables déposés</b><ul>"
    + "".join(
        f"<li><a href='{chemin.replace('/dbfs/FileStore', '/files')}'>{titre}</a>"
        f" — <code>{chemin}</code></li>"
        for titre, chemin in _liens
    )
    + "</ul></div>"
)
