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

Usage (notebook Databricks) :
    from modules.analysis import restituer_graphiques
    figs = restituer_graphiques(df_result)                  # affiche + PNG DBFS
    figs = restituer_graphiques(df_result, save_dir=None)   # affiche seulement
"""

import os
import logging

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame

from modules.kpi_export import compute_synthese
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
    ax.grid(axis="x" if ax.get_xlabel() else "y", alpha=0.25, linewidth=0.6)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=9)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=9)
    ax.tick_params(labelsize=9)


def _title(fig, message: str, contexte: str):
    """Titre-message (la conclusion) + sous-titre (le périmètre/contexte)."""
    fig.suptitle(message, fontsize=12, fontweight="bold", x=0.01, ha="left")
    fig.text(0.01, 0.915, contexte, fontsize=9, color="#555555", ha="left")


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
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.6))
    for ax, idx, lbl in ((axes[0], 1, "Dossiers"), (axes[1], 2, "PM compte")):
        left = 0.0
        total = sum(c[idx] for c in cats) or 1.0
        for name, *vals in [(c[0], c[1], c[2], c[3]) for c in cats]:
            v = (name, *vals)
            width = v[idx] / total * 100
            ax.barh([0], [width], left=left, color=v[3],
                    label=name if idx == 1 else None)
            if width > 6:
                txt = _n(v[1]) if idx == 1 else _meur(v[2])
                ax.text(left + width / 2, 0, txt, ha="center", va="center",
                        fontsize=8.5, color="white", fontweight="bold")
            left += width
        ax.set_xlim(0, 100)
        ax.set_yticks([])
        ax.set_xlabel(f"{lbl} (% du compte)", fontsize=9)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(labelsize=8)
    fig.legend(loc="lower center", ncol=5, fontsize=8.5, frameon=False)
    _title(
        fig,
        f"Justification du compte client : {_pct(d['taux_recup_global'])} des dossiers "
        f"rapprochés d'un inventaire (courant ou N+1)",
        f"Compte = {_n(d['cpt_nb'])} dossiers / {_meur(d['cpt_pm'])} de PM — "
        f"anomalies résiduelles : {_n(d['def_nb'])} dossiers ({_meur(d['def_pm'])}) — "
        f"date d'inventaire {d['date_inventaire']}",
    )
    fig.subplots_adjust(top=0.82, bottom=0.28)
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
    fig, ax = plt.subplots(figsize=(9, 3.8))
    labels = [b[0] for b in bars]
    vals   = [b[1] for b in bars]
    ax.barh(labels[::-1], vals[::-1], color=[b[3] for b in bars][::-1], height=0.6)
    for i, b in enumerate(bars[::-1]):
        txt = _n(b[1]) + (f"  ({_meur(b[2])} de PM MRM)" if b[2] is not None else " dossiers")
        ax.text(b[1] + max(vals) * 0.01, i, txt, va="center", fontsize=9)
    ax.set_xlim(0, max(vals) * 1.35)
    _style(ax, xlabel="Nombre de dossiers")
    _title(
        fig,
        f"Listes d'arrêts de travail : {_pct(d['taux_couverture_mrm'])} de la revue MRM "
        f"retrouvée au compte",
        f"Revue à comparer = {_n(d['a_comparer_nb'])} dossiers — non mappés : "
        f"{_n(d['non_mappes_nb'])} dossiers / {_meur(d['non_mappes_pm'])} de PM MRM "
        f"à instruire (consignes « à supprimer » exclues)",
    )
    fig.subplots_adjust(top=0.80, left=0.24)
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

    fig, ax = plt.subplots(figsize=(9.5, 0.45 * len(pdf) + 2.2))
    ax.barh(labels, pdf["taux_chute_pct"], color=colors, height=0.6)
    for i, (v, e, p) in enumerate(zip(pdf["taux_chute_pct"], pdf["ecart_signe"], pdf["poids_pm_pct"])):
        ax.text(v + (0.4 if v >= 0 else -0.4), i,
                f"{str(v).replace('.', ',')} %  (écart {_meur(e)}, poids {str(p).replace('.', ',')} %)",
                va="center", ha="left" if v >= 0 else "right", fontsize=8.5)
    ax.axvline(0, color="#333333", linewidth=0.8)
    ax.axvline(d["taux_chute_global"], color=C_GRIS, linewidth=1.2, linestyle="--")
    ax.text(d["taux_chute_global"], len(pdf) - 0.2,
            f" global {_pct(d['taux_chute_global'])}", fontsize=8.5, color="#555555")
    lo = min(float(pdf["taux_chute_pct"].min()), 0)
    hi = max(float(pdf["taux_chute_pct"].max()), 0)
    ax.set_xlim(lo - (hi - lo) * 0.55 - 1, hi + (hi - lo) * 0.55 + 1)
    _style(ax, xlabel="Taux de chute (%) — positif = sous-provisionné (risque), négatif = sur-provisionné")
    _title(
        fig,
        f"Provisionnement par clause : taux de chute global {_pct(d['taux_chute_global'])} "
        f"(écart {_meur(d['metrics_pm_ecart'])})",
        f"Top {len(pdf)} clauses par PM MRM — univers matchés + récupérés N+1, "
        f"consignes à conserver / étudier / ajouter ({_n(d['metrics_nb'])} dossiers)",
    )
    fig.subplots_adjust(top=0.86, left=0.18)
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

    fig, ax = plt.subplots(figsize=(8.5, 3.8))
    ax.bar(labels, taux, color=colors, width=0.55)
    for i, (_, v) in enumerate(consignes):
        ax.text(i, taux[i] + (0.6 if taux[i] >= 0 else -1.4),
                f"{str(v['taux_chute']).replace('.', ',')} %\n"
                f"PM MRM {_meur(v['pm_mrm'])}",
                ha="center", fontsize=8.5)
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.axhline(d["taux_chute_global"], color=C_GRIS, linewidth=1.2, linestyle="--")
    ax.text(len(labels) - 0.5, d["taux_chute_global"],
            f" global {_pct(d['taux_chute_global'])}", fontsize=8.5, color="#555555", va="bottom")
    _style(ax, ylabel="Taux de chute (%)")
    _title(
        fig,
        "Le taux de chute global est la moyenne pondérée (par la PM MRM) des consignes",
        "Positif = sous-provisionné (risque) — univers matchés + récupérés N+1 ; "
        "« à supprimer » suivie à part (la PM doit disparaître, pas être comparée)",
    )
    fig.subplots_adjust(top=0.82)
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

    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.barh(labels, conf, color=C_VERT, height=0.55, label="Conforme")
    ax.barh(labels, nonc, left=conf, color=C_ROUGE, height=0.55, label="Non conforme")
    for i, (k, v) in enumerate(items):
        ax.text(min(v["pct"], 94) / 2, i, f"{str(v['pct']).replace('.', ',')} %",
                ha="center", va="center", fontsize=9, color="white", fontweight="bold")
        ax.text(101, i, f"{_n(v['conf'])} / {_n(v['nb'])} dossiers", va="center", fontsize=8.5)
    ax.set_xlim(0, 132)
    ax.set_xticks([0, 25, 50, 75, 100])
    fig.legend(loc="lower center", ncol=2, fontsize=8.5, frameon=False)
    _style(ax, xlabel="Part des dossiers conformes (%)")
    _title(
        fig,
        f"Application des consignes de la revue : {_pct(d['conformite_globale'])} de "
        f"conformité globale (hors « à supprimer »)",
        "Conserver / étudier / ajouter : conforme = retrouvé au compte — "
        "À supprimer : conforme = effectivement absent du compte",
    )
    fig.subplots_adjust(top=0.80, left=0.16, bottom=0.26)
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

    fig, ax = plt.subplots(figsize=(9.5, 3.8))
    ax.bar(pdf["MOIS_LABEL"], pdf["PM"] / 1e6, color=colors, width=0.65)
    for x, (pm, nb) in enumerate(zip(pdf["PM"], pdf["NB"])):
        ax.text(x, pm / 1e6, f"{_n(nb)}", ha="center", va="bottom", fontsize=8)
    _style(ax, ylabel="PM compte (M€)")
    _title(
        fig,
        f"Anomalies résiduelles : {_n(d['def_nb'])} dossiers sans contrepartie MRM "
        f"({_meur(d['def_pm'])}), dont {round(pm_fin / pm_total * 100)} % de la PM "
        f"survenus en fin d'année",
        "CPT_ONLY définitifs par mois de survenance (étiquette = nb de dossiers) — "
        "Oct-Déc en rouge : déclarations tardives probables, à instruire en priorité",
    )
    fig.subplots_adjust(top=0.80)
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
    Construit les 6 graphiques de restitution, les affiche (notebook) et les
    écrit en PNG (save_dir, DBFS). save_dir=None → pas d'écriture.

    Returns:
        dict {nom: Figure} — réutilisable (insertion Excel/PowerPoint).
    """
    d = compute_synthese(df_result)
    df_result = derive_clause_column(df_result)

    figs = {
        "1_compte_justification" : graph_compte_justification(d),
        "2_couverture_mrm"       : graph_couverture_mrm(d),
        "3_chute_par_clause"     : graph_chute_par_clause(df_result, d),
        "4_chute_par_consigne"   : graph_chute_par_consigne(d),
        "5_conformite_consignes" : graph_conformite_consignes(d),
        "6_anomalies_cpt_only"   : graph_anomalies_cpt_only(df_result, d),
    }

    if save_dir:
        out = _to_local(save_dir)
        os.makedirs(out, exist_ok=True)
        for name, fig in figs.items():
            path = f"{out}/{name}.png"
            fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
            print(f"  ✓ [PNG]     {path}")

    if show:
        plt.show()
    return figs
