"""
Restitution graphique des analyses (matplotlib) — titres porteurs de message.

Chaque graphique répond à une question de la problématique de fiabilisation
(à destination de la direction financière et des engagements) :
    1. compte_justification   — le compte client est-il justifié par la revue ?
    2. couverture_mrm         — challenge des listes d'arrêts de travail :
                                quelle part de la revue MRM est au compte ?
    3. chute_par_clause       — challenge du provisionnement : quelles clauses
                                portent l'écart de provision ?
    4. chute_par_consigne     — l'écart de provision selon la consigne de la revue
    5. conformite_consignes   — les consignes de la revue sont-elles appliquées ?
    6. anomalies_cpt_only     — les anomalies résiduelles : volume, PM, saisonnalité
    7. kpi_chute_globale      — LE ratio de chute global (gros chiffre + PM en regard)
    8. kpi_conformite_globale — LE ratio de suivi des consignes au global (donut)

Usage (notebook Databricks) :
    from modules.analysis import restituer_graphiques
    figs = restituer_graphiques(df_result)                  # affiche + PNG DBFS
    figs = restituer_graphiques(df_result, save_dir=None)   # affiche seulement
"""

import os
import logging
import textwrap

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame

from modules.kpi_export import compute_synthese, kas_totaux
from modules.analysis.helpers import derive_clause_column
from modules.analysis.taux_chute import analyze_taux_chute_par_clause
from modules.analysis.orphelins import analyse_cpt_only
from modules.analysis.export import DEFAULT_BASE_PATH, _clause_dir, _to_local

logger = logging.getLogger(__name__)

# Palette restitution (codes AXA + sémantique risque)
C_BLEU    = "#00008F"   # référence / matchés inventaire
C_BLEU2   = "#4976BA"   # récupérés N+1
C_GRIS    = "#9A9A9A"   # hors métriques (récupérés via NON)
C_ORANGE  = "#F07662"   # explicable (obs tardives) / à étudier
C_ROUGE   = "#C91432"   # risque : anomalies, sous-provisionnement, non conforme
C_VERT    = "#138636"   # conforme / couvert

# Typographie commune (lisibilité slide / écran partagé)
F_TITRE   = 15          # titre-message
F_SST     = 11          # sous-titre (contexte/périmètre)
F_AXE     = 11.5        # labels d'axes et catégories
F_TXT     = 11          # annotations de valeurs
F_LEG     = 11          # légendes

GRAPHS_DIR_DEFAULT = f"{_clause_dir(DEFAULT_BASE_PATH)}/graphiques"


def _n(x) -> str:
    """Entier, séparateur de milliers espace (style FR)."""
    return f"{int(round(x or 0)):,}".replace(",", " ")


def _meur(x) -> str:
    """Montant en millions d'euros, une décimale, virgule FR."""
    return f"{(x or 0) / 1e6:.1f}".replace(".", ",") + " M€"


def _pct(x) -> str:
    """Pourcentage en format FR (virgule décimale)."""
    return str(x).replace(".", ",") + " %"


def _style(ax, xlabel: str = "", ylabel: str = ""):
    """Style commun : épuré, grille discrète, pas de cadre."""
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="x" if xlabel else "y", alpha=0.25, linewidth=0.6)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=F_AXE, labelpad=8)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=F_AXE, labelpad=8)
    ax.tick_params(labelsize=F_AXE)


def _title(fig, message: str, contexte: str):
    """Titre-message (la conclusion) + sous-titre aéré sur 1-2 lignes."""
    fig.suptitle(message, fontsize=F_TITRE, fontweight="bold",
                 x=0.02, y=0.985, ha="left")
    fig.text(0.02, 0.905, textwrap.fill(contexte, 110), fontsize=F_SST,
             color="#555555", ha="left", va="top", linespacing=1.5)


# ============================================================================
# 1. JUSTIFICATION DU COMPTE CLIENT
# ============================================================================

def graph_compte_justification(d: dict):
    """Le compte client est-il justifié par la revue d'inventaire ?"""
    cats = [
        ("Matchés inventaire",   d["match_nb"], d["match_pm_cpt"], C_BLEU),
        ("Récupérés N+1",        d["late_nb"],  d["late_pm"],      C_BLEU2),
        ("Récupérés via NON",    d["recup_non_nb"], d["recup_non_pm"], C_GRIS),
        ("Clos avant inv. N+1",  d["obs_nb"],   d["obs_pm"],       C_ORANGE),
        ("Anomalies (CPT_ONLY)", d["def_nb"],   d["def_pm"],       C_ROUGE),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.8))
    for ax, idx, lbl in ((axes[0], 1, "Dossiers"), (axes[1], 2, "PM compte")):
        left = 0.0
        total = sum(c[idx] for c in cats) or 1.0
        for c in cats:
            width = c[idx] / total * 100
            ax.barh([0], [width], left=left, color=c[3], height=0.5,
                    label=c[0] if idx == 1 else None)
            if width > 7:
                txt = _n(c[1]) if idx == 1 else _meur(c[2])
                ax.text(left + width / 2, 0, txt, ha="center", va="center",
                        fontsize=F_TXT, color="white", fontweight="bold")
            left += width
        ax.set_xlim(0, 100)
        ax.set_yticks([])
        ax.set_xlabel(f"{lbl} (% du compte)", fontsize=F_AXE, labelpad=8)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(labelsize=F_AXE - 1)
    fig.legend(loc="lower center", ncol=5, fontsize=F_LEG, frameon=False,
               columnspacing=1.6, handlelength=1.4)
    _title(
        fig,
        f"Justification du compte client : {_pct(d['taux_recup_global'])} des dossiers "
        f"rapprochés d'un inventaire (courant ou N+1)",
        f"Compte = {_n(d['cpt_nb'])} dossiers / {_meur(d['cpt_pm'])} de PM — "
        f"anomalies résiduelles : {_n(d['def_nb'])} dossiers ({_meur(d['def_pm'])}) — "
        f"date d'inventaire {d['date_inventaire']}",
    )
    fig.subplots_adjust(top=0.72, bottom=0.30, left=0.04, right=0.97, wspace=0.12)
    return fig


# ============================================================================
# 2. COUVERTURE DE LA REVUE MRM (challenge des listes d'arrêts de travail)
# ============================================================================

def graph_couverture_mrm(d: dict):
    """Quelle part de la revue MRM (hors « à supprimer ») est retrouvée au compte ?"""
    bars = [
        ("Matchés au compte",        d["match_nb"], None,           C_VERT),
        ("Non mappés — à conserver", d["keep_nb"],  d["keep_pm"],   C_ROUGE),
        ("Non mappés — à étudier",   d["study_nb"], d["study_pm"],  C_ORANGE),
        ("Non mappés — à ajouter",   d["add_nb"],   d["add_pm"],    C_ORANGE),
    ]
    fig, ax = plt.subplots(figsize=(12, 5.2))
    labels = [b[0] for b in bars]
    vals   = [b[1] for b in bars]
    ax.barh(labels[::-1], vals[::-1], color=[b[3] for b in bars][::-1], height=0.55)
    for i, b in enumerate(bars[::-1]):
        txt = _n(b[1]) + (f"   ({_meur(b[2])} de PM MRM)" if b[2] is not None else " dossiers")
        ax.text(b[1] + max(vals) * 0.015, i, txt, va="center", fontsize=F_TXT)
    ax.set_xlim(0, max(vals) * 1.45)
    _style(ax, xlabel="Nombre de dossiers")
    _title(
        fig,
        f"Listes d'arrêts de travail : {_pct(d['taux_couverture_mrm'])} de la revue MRM "
        f"retrouvée au compte",
        f"Revue à comparer = {_n(d['a_comparer_nb'])} dossiers — non mappés : "
        f"{_n(d['non_mappes_nb'])} dossiers / {_meur(d['non_mappes_pm'])} de PM MRM "
        f"à instruire (consignes « à supprimer » exclues)",
    )
    fig.subplots_adjust(top=0.74, bottom=0.14, left=0.24, right=0.97)
    return fig


# ============================================================================
# 3. TAUX DE CHUTE PAR CLAUSE (challenge du provisionnement)
# ============================================================================

def graph_chute_par_clause(df_result: DataFrame, d: dict, top: int = 12):
    """Quelles clauses portent l'écart de provisionnement ?"""
    pdf = (
        analyze_taux_chute_par_clause(df_result).toPandas()
        .sort_values("pm_mrm", ascending=False)
        .head(top)[::-1]
    )
    labels = [f"{c} ({t})" for c, t in zip(pdf["CLAUSE"], pdf["TYPE_CLAUSE"])]
    colors = [C_ROUGE if v > 0 else C_BLEU for v in pdf["taux_chute_pct"]]

    h = 0.6 * len(pdf) + 3.2
    fig, ax = plt.subplots(figsize=(12, h))
    ax.barh(labels, pdf["taux_chute_pct"], color=colors, height=0.55)
    for i, (v, e, p) in enumerate(zip(pdf["taux_chute_pct"], pdf["ecart_signe"], pdf["poids_pm_pct"])):
        ax.text(v + (0.5 if v >= 0 else -0.5), i,
                f"{_pct(v)}   (écart {_meur(e)}, poids {_pct(p)})",
                va="center", ha="left" if v >= 0 else "right", fontsize=F_TXT - 1)
    ax.axvline(0, color="#333333", linewidth=0.8)
    ax.axvline(d["taux_chute_global"], color=C_GRIS, linewidth=1.4, linestyle="--")
    ax.text(d["taux_chute_global"], len(pdf) - 0.1,
            f" global {_pct(d['taux_chute_global'])}", fontsize=F_TXT, color="#555555")
    lo = min(float(pdf["taux_chute_pct"].min()), 0)
    hi = max(float(pdf["taux_chute_pct"].max()), 0)
    ax.set_xlim(lo - (hi - lo) * 0.65 - 2, hi + (hi - lo) * 0.65 + 2)
    _style(ax, xlabel="Taux de chute (%) — positif = sous-provisionné (risque), négatif = sur-provisionné")
    _title(
        fig,
        f"Provisionnement par clause : taux de chute global {_pct(d['taux_chute_global'])} "
        f"(écart {_meur(d['metrics_pm_ecart'])})",
        f"Top {len(pdf)} clauses par PM MRM — univers matchés + récupérés N+1, "
        f"consignes à conserver / étudier / ajouter ({_n(d['metrics_nb'])} dossiers)",
    )
    fig.subplots_adjust(top=max(0.78, 1 - 1.5 / h), bottom=1.1 / h, left=0.18, right=0.97)
    return fig


# ============================================================================
# 4. TAUX DE CHUTE PAR CONSIGNE
# ============================================================================

def graph_chute_par_consigne(d: dict):
    """L'écart de provision selon la consigne posée par la revue."""
    consignes = [(k, v) for k, v in d["consignes"].items() if v["pertinent"]]
    labels = [k for k, _ in consignes]
    taux   = [v["taux_chute"] for _, v in consignes]
    colors = [C_ROUGE if t > 0 else C_BLEU for t in taux]

    fig, ax = plt.subplots(figsize=(10, 5.6))
    ax.bar(labels, taux, color=colors, width=0.5)
    span = max(taux) - min(taux) or 1
    for i, (_, v) in enumerate(consignes):
        au_dessus = taux[i] >= 0
        ax.text(i, taux[i] + (span * 0.04 if au_dessus else -span * 0.04),
                f"{_pct(v['taux_chute'])}\nPM MRM {_meur(v['pm_mrm'])}",
                ha="center", va="bottom" if au_dessus else "top",
                fontsize=F_TXT, linespacing=1.5)
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.axhline(d["taux_chute_global"], color=C_GRIS, linewidth=1.4, linestyle="--",
               label=f"taux de chute global : {_pct(d['taux_chute_global'])}")
    ax.margins(y=0.22)
    ax.legend(loc="lower left", fontsize=F_LEG, frameon=False)
    _style(ax, ylabel="Taux de chute (%)")
    _title(
        fig,
        "Le taux de chute global est la moyenne pondérée (par la PM MRM) des consignes",
        "Positif = sous-provisionné (risque) — univers matchés + récupérés N+1 ; "
        "« à supprimer » suivie à part (la PM doit disparaître, pas être comparée)",
    )
    fig.subplots_adjust(top=0.76, bottom=0.10, left=0.10, right=0.96)
    return fig


# ============================================================================
# 5. CONFORMITÉ DES CONSIGNES
# ============================================================================

def graph_conformite_consignes(d: dict):
    """Les consignes de la revue sont-elles appliquées au compte ?"""
    items = list(d["consignes"].items())[::-1]
    labels = [k for k, _ in items]
    conf   = [v["pct"] for _, v in items]
    nonc   = [round(100 - v["pct"], 1) for _, v in items]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.barh(labels, conf, color=C_VERT, height=0.5, label="Conforme")
    ax.barh(labels, nonc, left=conf, color=C_ROUGE, height=0.5, label="Non conforme")
    for i, (k, v) in enumerate(items):
        ax.text(min(v["pct"], 92) / 2, i, _pct(v["pct"]),
                ha="center", va="center", fontsize=F_TXT,
                color="white", fontweight="bold")
        ax.text(103, i, f"{_n(v['conf'])} / {_n(v['nb'])} dossiers",
                va="center", fontsize=F_TXT)
    ax.set_xlim(0, 140)
    ax.set_xticks([0, 25, 50, 75, 100])
    fig.legend(loc="lower center", ncol=2, fontsize=F_LEG, frameon=False,
               columnspacing=2.0)
    _style(ax, xlabel="Part des dossiers conformes (%)")
    _title(
        fig,
        f"Application des consignes de la revue : {_pct(d['conformite_globale'])} de "
        f"conformité globale (hors « à supprimer »)",
        "Conserver / étudier / ajouter : conforme = retrouvé au compte — "
        "À supprimer : conforme = effectivement absent du compte",
    )
    fig.subplots_adjust(top=0.74, bottom=0.26, left=0.15, right=0.97)
    return fig


# ============================================================================
# 6. ANOMALIES RÉSIDUELLES CPT_ONLY (saisonnalité)
# ============================================================================

def graph_anomalies_cpt_only(df_result: DataFrame, d: dict):
    """Volume / PM des anomalies par mois de survenance — effet fin d'année."""
    pdf = (
        analyse_cpt_only(df_result).toPandas()
        .groupby(["MOIS_SURVENANCE", "MOIS_LABEL"], as_index=False)
        .agg(NB=("NB_DOSSIERS", "sum"), PM=("PM_CPT_TOTAL", "sum"))
        .sort_values("MOIS_SURVENANCE")
    )
    pm_total = float(pdf["PM"].sum()) or 1.0
    pm_fin   = float(pdf.loc[pdf["MOIS_SURVENANCE"].isin([10, 11, 12]), "PM"].sum())
    colors   = [C_ROUGE if m in (10, 11, 12) else C_GRIS for m in pdf["MOIS_SURVENANCE"]]

    fig, ax = plt.subplots(figsize=(12, 5.2))
    ax.bar(pdf["MOIS_LABEL"], pdf["PM"] / 1e6, color=colors, width=0.6)
    for x, (pm, nb) in enumerate(zip(pdf["PM"], pdf["NB"])):
        ax.text(x, pm / 1e6, f"{_n(nb)}", ha="center", va="bottom", fontsize=F_TXT - 1)
    ax.margins(y=0.14)
    _style(ax, ylabel="PM compte (M€)")
    _title(
        fig,
        f"Anomalies résiduelles : {_n(d['def_nb'])} dossiers sans contrepartie MRM "
        f"({_meur(d['def_pm'])}), dont {round(pm_fin / pm_total * 100)} % de la PM "
        f"survenus en fin d'année",
        "CPT_ONLY définitifs par mois de survenance (étiquette = nb de dossiers) — "
        "Oct-Déc en rouge : déclarations tardives probables, à instruire en priorité",
    )
    fig.subplots_adjust(top=0.76, bottom=0.10, left=0.08, right=0.97)
    return fig


# ============================================================================
# 7. KPI — TAUX DE CHUTE GLOBAL
# ============================================================================

def graph_kpi_chute_globale(d: dict):
    """Le ratio de chute global en un visuel : gros chiffre + PM en regard."""
    k = kas_totaux(d)
    val = d["taux_chute_global"]
    sous_prov = val > 0
    couleur = C_ROUGE if sous_prov else C_BLEU

    fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(13, 4.6), width_ratios=[1, 1.5])
    ax0.text(0.5, 0.62, _pct(val), ha="center", va="center",
             fontsize=42, fontweight="bold", color=couleur)
    ax0.text(0.5, 0.30,
             "sous-provisionnement (risque)" if sous_prov else "sur-provisionnement (marge)",
             ha="center", fontsize=F_AXE, color="#555555")
    ax0.axis("off")

    bars = [("PM revue MRM", k["pm_mrm"], C_BLEU), ("PM compte client", k["pm_cpt"], C_GRIS)]
    ax1.barh([b[0] for b in bars][::-1], [b[1] / 1e6 for b in bars][::-1],
             color=[b[2] for b in bars][::-1], height=0.45)
    for i, b in enumerate(bars[::-1]):
        ax1.text(b[1] / 1e6 + max(k["pm_mrm"], k["pm_cpt"]) / 1e6 * 0.02, i,
                 _meur(b[1]), va="center", fontsize=F_TXT + 1, fontweight="bold")
    ax1.set_xlim(0, max(k["pm_mrm"], k["pm_cpt"]) / 1e6 * 1.28)
    ax1.set_xticks([])
    fig.text(0.52, 0.07, f"Écart (PM MRM − PM compte) : {_meur(k['delta'])}",
             fontsize=F_TXT + 1, fontweight="bold", color=couleur)
    _style(ax1)
    ax1.grid(False)
    _title(
        fig,
        f"Taux de chute global : {_pct(val)} — le compte porte "
        f"{_meur(abs(k['delta']))} de {'moins' if sous_prov else 'plus'} que la revue",
        "Σ(PM MRM − PM CPT) / Σ PM MRM — univers matchés + récupérés N+1, consignes "
        "à conserver / étudier / ajouter (« à supprimer » suivie à part)",
    )
    fig.subplots_adjust(top=0.72, bottom=0.18, left=0.03, right=0.96, wspace=0.30)
    return fig


# ============================================================================
# 8. KPI — CONFORMITÉ GLOBALE DES CONSIGNES
# ============================================================================

def graph_kpi_conformite_globale(d: dict):
    """Le ratio de suivi des consignes au global : donut conformes / non conformes."""
    k = kas_totaux(d)
    conf, non_conf = k["conf"], k["nb"] - k["conf"]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.pie(
        [conf, non_conf], colors=[C_VERT, C_ROUGE], startangle=90,
        counterclock=False, wedgeprops=dict(width=0.36, edgecolor="white"),
    )
    ax.text(0, 0.10, _pct(d["conformite_globale"]), ha="center", va="center",
            fontsize=32, fontweight="bold", color=C_VERT)
    ax.text(0, -0.28, "de consignes\nappliquées", ha="center", va="center",
            fontsize=F_AXE, color="#555555", linespacing=1.5)
    ax.legend(
        [f"Conformes (retrouvées au compte) — {_n(conf)}",
         f"Non conformes (non retrouvées) — {_n(non_conf)}"],
        loc="center left", bbox_to_anchor=(1.05, 0.5), fontsize=F_LEG,
        frameon=False, labelspacing=1.2,
    )
    _title(
        fig,
        f"Suivi des consignes de la revue : {_pct(d['conformite_globale'])} "
        f"appliquées au compte ({_n(conf)} / {_n(k['nb'])} dossiers)",
        "Consignes à conserver / étudier / ajouter, toutes clauses confondues — "
        "« à supprimer » jugée à part (conforme = effectivement absente du compte)",
    )
    fig.subplots_adjust(top=0.74, bottom=0.06, left=0.02, right=0.52)
    return fig


# ============================================================================
# ORCHESTRATEUR
# ============================================================================

def restituer_graphiques(
    df_result: DataFrame,
    save_dir : str = GRAPHS_DIR_DEFAULT,
    show     : bool = True,
) -> dict:
    """
    Construit les 8 graphiques de restitution, les affiche (notebook) et les
    écrit en PNG (save_dir, DBFS). save_dir=None → pas d'écriture.

    Returns:
        dict {nom: Figure} — réutilisable (insertion Excel/PowerPoint).
    """
    d = compute_synthese(df_result)
    df_result = derive_clause_column(df_result)

    figs = {
        "1_compte_justification"   : graph_compte_justification(d),
        "2_couverture_mrm"         : graph_couverture_mrm(d),
        "3_chute_par_clause"       : graph_chute_par_clause(df_result, d),
        "4_chute_par_consigne"     : graph_chute_par_consigne(d),
        "5_conformite_consignes"   : graph_conformite_consignes(d),
        "6_anomalies_cpt_only"     : graph_anomalies_cpt_only(df_result, d),
        "7_kpi_chute_globale"      : graph_kpi_chute_globale(d),
        "8_kpi_conformite_globale" : graph_kpi_conformite_globale(d),
    }

    if save_dir:
        out = _to_local(save_dir)
        os.makedirs(out, exist_ok=True)
        for name, fig in figs.items():
            path = f"{out}/{name}.png"
            fig.savefig(path, dpi=150, facecolor="white")
            print(f"  ✓ [PNG]     {path}")

    if show:
        plt.show()
    return figs
