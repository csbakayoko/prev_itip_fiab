"""
Restitution graphique des métriques (matplotlib) — titres porteurs de message.

Consomme exclusivement la couche métriques (core.metrics) : viz ne calcule
rien, elle met en forme. Chaque graphique prend `d` (le dict de
compute_synthese, une seule passe Spark) ; les graphes 3 et 6 prennent en plus
la table pandas de leur métrique. Chaque graphique répond à une question de la
problématique de fiabilisation (direction financière et engagements) :
    1. compte_justification   — le compte client est-il justifié par la revue ?
    2. couverture_mrm         — challenge des listes d'arrêts de travail :
                                quelle part de la revue MRM est au compte ?
    3. chute_par_type_compte  — challenge du provisionnement : quels types de
                                compte portent l'écart ? (inventaire courant)
    4. chute_par_consigne     — l'écart de provision selon la consigne de la revue
    5. conformite_consignes   — les consignes de la revue sont-elles appliquées ?
    6. anomalies_cpt_only     — les anomalies résiduelles : volume, PM, saisonnalité
    7. kpi_chute              — LE taux de chute (matchés inventaire courant,
                                gros chiffre + PM ; N+1 rappelé à part)
    8. kpi_conformite_globale — LE ratio de suivi des consignes au global (donut)
    9. pm_par_consigne        — PM revue vs PM compte par consigne (Δ en € et en %)
   10. chute_par_anciennete   — chute par année de survenance (N / N-1 / N-2+) :
                                la méthode d'inventaire diffère selon l'année
   11. orphelins_par_compte   — quel compte PB concentre les orphelins (souscripteur
                                à investiguer)
   12. distribution_ecarts    — combien de dossiers sur/sous-provisionnés, et à
                                quelle ampleur ? (tranches de SEUILS_ECART_PM)

Usage (notebook Databricks) :
    from core.metrics.viz import restituer_graphiques

    d = print_synthese(df_result)                          # la passe Spark
    figs = restituer_graphiques(df_result, d)              # affiche + PNG DBFS
    figs = restituer_graphiques(df_result, d, save_dir=None)  # affiche seulement
"""

import os

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from pyspark.sql import DataFrame

from core.synthese.kpi_export import compute_synthese, kas_totaux
from core.metrics import (
    chute_par_type_compte, chute_par_anciennete, chute_par_tranche_ecart,
    anomalies_cpt_only, orphelins_par_clause, output_dir, _to_local,
    EXERCICE_INV, TRANCHE_ECART_NUL, _annee_inventaire,
)

# Palette AXA en priorité, complétée quand la sémantique l'exige.
C_BLEU   = "#00008F"   # AXA Blue   — référence (revue MRM, matchés)
C_OCEAN  = "#4976BA"   # AXA Ocean  — compte / récupérés N+1 / sur-provisionné
C_TEAL   = "#027180"   # AXA Teal   — conforme / couvert
C_SIENNE = "#F07662"   # AXA Sienna — sous-provisionné, à étudier (risque modéré)
C_ROUGE  = "#FF1721"   # AXA Red    — anomalies / consigne non suivie (alerte)
C_GRIS   = "#999999"   # neutre     — hors métriques / contexte

# Typographie commune (lisibilité slide / écran partagé)
F_TITRE  = 15          # titre-message
F_SST    = 10          # sous-titre (contexte/périmètre) — 1 ligne, italique
F_AXE    = 11.5        # labels d'axes et catégories
F_TXT    = 11          # annotations de valeurs
F_LEG    = 11          # légendes

GRAPHS_DIR_DEFAULT = output_dir(sub="graphiques")


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
    """Titre-message (la conclusion) + sous-titre court, 1 ligne, italique."""
    fig.suptitle(message, fontsize=F_TITRE, fontweight="bold",
                 x=0.02, y=0.97, ha="left")
    fig.text(0.02, 0.885, contexte, fontsize=F_SST, color="#555555",
             ha="left", style="italic")


# ============================================================================
# 1. JUSTIFICATION DU COMPTE CLIENT
# ============================================================================

def graph_compte_justification(d: dict):
    """Le compte client est-il justifié par la revue d'inventaire ?"""
    cats = [
        ("Retrouvés (inventaire)",     d["match_nb"], d["match_pm_cpt"], C_BLEU),
        ("Retrouvés via N+1",          d["late_nb"],  d["late_pm"],      C_OCEAN),
        ("Repêchés (statut MRM non)",  d["recup_non_nb"], d["recup_non_pm"], C_GRIS),
        ("Clos avant inv. N+1",        d["obs_nb"],   d["obs_pm"],       C_SIENNE),
        ("Sans contrepartie (anom.)",  d["def_nb"],   d["def_pm"],       C_ROUGE),
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
        f"retrouvés à un inventaire (courant ou N+1)",
        f"Compte = {_n(d['cpt_nb'])} dossiers / {_meur(d['cpt_pm'])} de PM — anomalies "
        f"résiduelles : {_n(d['def_nb'])} ({_meur(d['def_pm'])}) — inventaire {d['date_inventaire']}",
    )
    fig.subplots_adjust(top=0.74, bottom=0.30, left=0.04, right=0.97, wspace=0.12)
    return fig


# ============================================================================
# 2. COUVERTURE DE LA REVUE MRM (challenge des listes d'arrêts de travail)
# ============================================================================

def graph_couverture_mrm(d: dict):
    """Quelle part de la revue MRM est retrouvée au compte ? (+ « à supprimer »
    retrouvées : consignes de suppression non suivies)."""
    base  = d["a_comparer_nb"] or 1
    c_del = d["consignes"]["À supprimer"]
    del_ko = c_del["nb"] - c_del["conf"]          # retrouvées alors qu'à supprimer
    pct = lambda nb, den: round(nb / (den or 1) * 100, 1)

    bars = [
        # (libellé, nb, pct affiché, PM MRM, couleur, hachures)
        ("Retrouvés au compte",      d["match_nb"], pct(d["match_nb"], base), None,          C_TEAL,   None),
        ("À conserver non retrouvé", d["keep_nb"],  pct(d["keep_nb"],  base), d["keep_pm"],  C_SIENNE, None),
        ("À étudier non retrouvé",   d["study_nb"], pct(d["study_nb"], base), d["study_pm"], C_SIENNE, None),
        ("À ajouter non retrouvé",   d["add_nb"],   pct(d["add_nb"],   base), d["add_pm"],   C_SIENNE, None),
        ("« À supprimer » retrouvées au compte",
                                     del_ko, pct(del_ko, c_del["nb"]), c_del["pm_mrm"], C_ROUGE, "//"),
    ]
    fig, ax = plt.subplots(figsize=(13, 6))
    vals = [b[1] for b in bars]
    for i, (lbl, nb, p, pm, coul, hach) in enumerate(bars[::-1]):
        ax.barh([lbl], [nb], color=coul, height=0.55, hatch=hach, edgecolor="white")
        txt = f"{_n(nb)} ({_pct(p)})" + (f" — {_meur(pm)} de PM MRM" if pm is not None else "")
        ax.text(nb + max(vals) * 0.015, i, txt, va="center", fontsize=F_TXT)
    ax.set_xlim(0, max(vals) * 1.50)
    _style(ax, xlabel="Nombre de dossiers — % de la revue à comparer ; "
                      "« à supprimer » : % de la consigne, PM = non supprimée")
    _title(
        fig,
        f"Listes d'arrêts de travail : {_pct(d['taux_couverture_mrm'])} de la revue MRM "
        f"retrouvée au compte",
        f"Revue à comparer = {_n(d['a_comparer_nb'])} dossiers — non retrouvés : "
        f"{_n(d['non_mappes_nb'])} / {_meur(d['non_mappes_pm'])} de PM MRM à instruire",
    )
    fig.subplots_adjust(top=0.80, bottom=0.14, left=0.27, right=0.97)
    return fig


# ============================================================================
# 3. TAUX DE CHUTE PAR CLAUSE (challenge du provisionnement)
# ============================================================================

def graph_chute_par_type_compte(pdf_types, d: dict):
    """Quels types de compte portent l'écart de provisionnement ?

    pdf_types = metrics.chute_par_type_compte(df_result), ventilée par
    EXERCICE. Barres = bloc « Inventaire courant » (les stats globales) ;
    les récupérés N+1 restent une analyse séparée (bloc dédié de la table)."""
    pdf = pdf_types[pdf_types["EXERCICE"] == EXERCICE_INV][::-1]
    labels = list(pdf["TYPE_COMPTE"].fillna("Type non renseigné"))
    colors = [C_SIENNE if v > 0 else C_OCEAN for v in pdf["TAUX_CHUTE_PCT"]]

    h = 0.6 * len(pdf) + 3.2
    fig, ax = plt.subplots(figsize=(12, h))
    ax.barh(labels, pdf["TAUX_CHUTE_PCT"], color=colors, height=0.55)
    for i, (v, e, p) in enumerate(zip(pdf["TAUX_CHUTE_PCT"], pdf["ECART"],
                                      pdf["POIDS_PM_PCT"])):
        ax.text(v + (0.5 if v >= 0 else -0.5), i,
                f"{_pct(v)}   (écart {_meur(e)}, poids {_pct(p)})",
                va="center", ha="left" if v >= 0 else "right", fontsize=F_TXT - 1)
    ax.axvline(0, color="#333333", linewidth=0.8, zorder=0.5)
    ax.axvline(d["taux_chute_inventaire"], color=C_GRIS, linewidth=1.4, linestyle="--",
               zorder=0.5,
               label=f"taux de chute : {_pct(d['taux_chute_inventaire'])}")
    ax.legend(loc="lower right", fontsize=F_LEG, frameon=False)
    lo = min(float(pdf["TAUX_CHUTE_PCT"].min()), 0)
    hi = max(float(pdf["TAUX_CHUTE_PCT"].max()), 0)
    ax.set_xlim(lo - (hi - lo) * 0.65 - 2, hi + (hi - lo) * 0.65 + 2)
    _style(ax, xlabel="Taux de chute (%) — positif = sous-provisionné (risque), négatif = sur-provisionné")
    _title(
        fig,
        f"Provisionnement par type de compte : taux de chute {_pct(d['taux_chute_inventaire'])} "
        f"(écart {_meur(d['metrics_pm_ecart'])})",
        f"{len(pdf)} type(s) de compte par PM MRM — matchés de l'inventaire courant, hors "
        f"« à supprimer » / statut NON ; N+1 : {_pct(d['taux_chute_n1'])} (analyse séparée)",
    )
    fig.subplots_adjust(top=max(0.80, 1 - 1.3 / h), bottom=1.1 / h, left=0.18, right=0.97)
    return fig


# ============================================================================
# 4. TAUX DE CHUTE PAR CONSIGNE
# ============================================================================

def graph_chute_par_consigne(d: dict):
    """L'écart de provision selon la consigne posée par la revue."""
    consignes = [(c, v) for c, v in d["consignes"].items() if v["pertinent"]]
    labels = [c for c, _ in consignes]
    taux   = [v["taux_chute"] for _, v in consignes]
    colors = [C_SIENNE if t > 0 else C_OCEAN for t in taux]

    fig, ax = plt.subplots(figsize=(11, 5.6))
    ax.bar(labels, taux, color=colors, width=0.5)
    span = max(taux) - min(taux) or 1
    for i, (_, v) in enumerate(consignes):
        au_dessus = taux[i] >= 0
        ax.text(i, taux[i] + (span * 0.06 if au_dessus else -span * 0.06),
                f"{_pct(v['taux_chute'])} — PM MRM {_meur(v['pm_mrm'])}",
                ha="center", va="bottom" if au_dessus else "top", fontsize=F_TXT)
    ax.axhline(0, color="#333333", linewidth=0.8, zorder=0.5)
    ax.axhline(d["taux_chute_inventaire"], color=C_GRIS, linewidth=1.4, linestyle="--",
               zorder=0.5,
               label=f"taux de chute inventaire : {_pct(d['taux_chute_inventaire'])}")
    ax.margins(y=0.26)
    ax.legend(loc="lower left", fontsize=F_LEG, frameon=False)
    _style(ax, ylabel="Taux de chute (%)")
    _title(
        fig,
        "Le taux de chute de l'exercice courant est la moyenne pondérée (par la PM MRM) "
        "des consignes",
        "Positif = sous-provisionné (risque) — matchés de l'inventaire courant, hors "
        "« à supprimer » / statut NON ; récupérés N+1 analysés à part",
    )
    fig.subplots_adjust(top=0.78, bottom=0.10, left=0.10, right=0.96)
    return fig


# ============================================================================
# 5. CONFORMITÉ DES CONSIGNES (toutes les consignes, « à supprimer » incluse)
# ============================================================================

def graph_conformite_consignes(d: dict):
    """Les consignes de la revue sont-elles appliquées au compte ?

    Le reste-à-100 % est qualifié selon la consigne : « non retrouvé » pour
    conserver / ajouter / étudier absents du compte, « encore au compte » pour
    les à supprimer non suivies."""
    items = list(d["consignes"].items())[::-1]
    labels = [c for c, _ in items]
    conf   = [v["pct"] for _, v in items]
    nonc   = [round(100 - v["pct"], 1) for _, v in items]
    # Couleur du KO selon sa nature : sienne = non retrouvé, rouge = encore au compte.
    ko_colors = [C_SIENNE if v["ko_label"] == "non retrouvé" else C_ROUGE
                 for _, v in items]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.barh(labels, conf, color=C_TEAL, height=0.5)
    for i, (c, v) in enumerate(items):
        ax.barh([c], [nonc[i]], left=conf[i], color=ko_colors[i], height=0.5)
        ax.text(min(v["pct"], 92) / 2, i, _pct(v["pct"]),
                ha="center", va="center", fontsize=F_TXT,
                color="white", fontweight="bold")
        ko_txt = f" — {_n(v['ko'])} {v['ko_label']}" if v["ko"] else ""
        ax.text(103, i, f"{_n(v['conf'])} / {_n(v['nb'])} dossiers{ko_txt}",
                va="center", fontsize=F_TXT)
    ax.set_xlim(0, 150)
    ax.set_xticks([0, 25, 50, 75, 100])
    handles = [
        Patch(color=C_TEAL,   label="Conforme (consigne respectée)"),
        Patch(color=C_SIENNE, label="Non retrouvé (absent du compte)"),
        Patch(color=C_ROUGE,  label="Encore au compte (à supprimer non suivie)"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=3, fontsize=F_LEG,
               frameon=False, columnspacing=1.6)
    _style(ax, xlabel="Part des dossiers conformes (%)")
    _title(
        fig,
        f"Application des consignes de la revue : {_pct(d['conformite_globale'])} de "
        f"conformité globale (hors « à supprimer »)",
        "Exercice courant (N+1 suivis à part) — conserver / ajouter / étudier : conforme = "
        "retrouvé au compte ; à supprimer : conforme = absent",
    )
    fig.subplots_adjust(top=0.78, bottom=0.26, left=0.15, right=0.97)
    return fig


# ============================================================================
# 6. ANOMALIES RÉSIDUELLES CPT_ONLY (saisonnalité)
# ============================================================================

def graph_anomalies_cpt_only(pdf, d: dict):
    """Volume / PM des anomalies par mois de survenance — effet fin d'année.

    pdf = metrics.anomalies_cpt_only(df_result)."""
    pm_total = float(pdf["PM_CPT"].sum()) or 1.0
    pm_fin   = float(pdf.loc[pdf["IS_FIN_ANNEE"], "PM_CPT"].sum())
    colors   = [C_ROUGE if fin else C_GRIS for fin in pdf["IS_FIN_ANNEE"]]

    fig, ax = plt.subplots(figsize=(12, 5.2))
    ax.bar(pdf["MOIS_LABEL"], pdf["PM_CPT"] / 1e6, color=colors, width=0.6)
    for x, (pm, nb) in enumerate(zip(pdf["PM_CPT"], pdf["NB_DOSSIERS"])):
        ax.text(x, pm / 1e6, f"{_n(nb)}", ha="center", va="bottom", fontsize=F_TXT - 1)
    ax.margins(y=0.14)
    _style(ax, ylabel="PM compte (M€)")
    _title(
        fig,
        f"Anomalies résiduelles : {_n(d['def_nb'])} dossiers sans contrepartie MRM "
        f"({_meur(d['def_pm'])}), dont {round(pm_fin / pm_total * 100)} % de la PM "
        f"survenus en fin d'année",
        "Dossiers compte sans contrepartie revue, par mois de survenance (étiquette = nb) — "
        "Oct-Déc : déclarations tardives probables",
    )
    fig.subplots_adjust(top=0.78, bottom=0.10, left=0.08, right=0.97)
    return fig


# ============================================================================
# 7. KPI — TAUX DE CHUTE
# ============================================================================

def graph_kpi_chute(d: dict):
    """LE taux de chute (matchés de l'inventaire courant) : gros chiffre + PM
    en regard ; les récupérés N+1 rappelés en analyse séparée."""
    pm_mrm, pm_cpt = d["metrics_pm_mrm"], d["metrics_pm_cpt"]
    delta = d["metrics_pm_ecart"]
    val = d["taux_chute_inventaire"]
    sous_prov = val > 0
    couleur = C_ROUGE if sous_prov else C_BLEU

    fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(13, 4.6), width_ratios=[1, 1.5])
    ax0.text(0.5, 0.68, _pct(val), ha="center", va="center",
             fontsize=42, fontweight="bold", color=couleur)
    ax0.text(0.5, 0.42,
             "sous-provisionnement (risque)" if sous_prov else "sur-provisionnement (marge)",
             ha="center", fontsize=F_AXE, color="#555555")
    ax0.text(0.5, 0.18,
             f"récupérés N+1 : {_pct(d['taux_chute_n1'])} (analyse séparée)",
             ha="center", fontsize=F_AXE - 1, color="#555555")
    ax0.axis("off")

    bars = [("PM revue MRM", pm_mrm, C_BLEU), ("PM compte client", pm_cpt, C_OCEAN)]
    ax1.barh([b[0] for b in bars][::-1], [b[1] / 1e6 for b in bars][::-1],
             color=[b[2] for b in bars][::-1], height=0.45)
    for i, b in enumerate(bars[::-1]):
        ax1.text(b[1] / 1e6 + max(pm_mrm, pm_cpt) / 1e6 * 0.02, i,
                 _meur(b[1]), va="center", fontsize=F_TXT + 1, fontweight="bold")
    ax1.set_xlim(0, max(pm_mrm, pm_cpt) / 1e6 * 1.28)
    ax1.set_xticks([])
    fig.text(0.52, 0.07, f"Écart (PM MRM − PM compte) : {_meur(delta)}",
             fontsize=F_TXT + 1, fontweight="bold", color=couleur)
    _style(ax1)
    ax1.grid(False)
    _title(
        fig,
        f"Taux de chute : {_pct(val)} — le compte porte "
        f"{_meur(abs(delta))} de {'moins' if sous_prov else 'plus'} que la revue",
        "Σ(PM MRM − PM CPT) / Σ PM MRM — matchés de l'inventaire courant, hors "
        "« à supprimer » / statut NON ; N+1 et repêchés statut NON analysés à part",
    )
    fig.subplots_adjust(top=0.76, bottom=0.18, left=0.03, right=0.96, wspace=0.30)
    return fig


# ============================================================================
# 8. KPI — CONFORMITÉ GLOBALE DES CONSIGNES
# ============================================================================

def _donut(ax, vals: list, colors: list, pct_centre: str, sous_label: str,
           caption: str):
    """Donut (2 ou 3 segments) avec taux au centre et légende dessous."""
    ax.pie(vals, colors=colors, startangle=90,
           counterclock=False, wedgeprops=dict(width=0.36, edgecolor="white"))
    ax.text(0, 0.10, pct_centre, ha="center", va="center",
            fontsize=28, fontweight="bold", color=C_TEAL)
    ax.text(0, -0.30, sous_label, ha="center", va="center",
            fontsize=F_AXE - 1, color="#555555", linespacing=1.5)
    ax.text(0, -1.45, caption, ha="center", va="center", fontsize=F_TXT)


def graph_kpi_conformite_globale(d: dict):
    """Suivi des consignes au global : conformité KAS + suppression effective.

    Donut gauche en 2 parts (conforme / non retrouvé) pour les consignes
    conserver/étudier/ajouter ; donut droit = suppression effective."""
    k    = kas_totaux(d)
    cons = d["consignes"]
    conf = k["conf"]
    nr   = (cons["À conserver"]["ko"] + cons["À ajouter"]["ko"]
            + cons["À étudier"]["ko"])          # non retrouvés
    c_del  = d["consignes"]["À supprimer"]
    del_ok = c_del["conf"]                      # effectivement supprimées
    del_ko = c_del["nb"] - del_ok               # retrouvées au compte (non suivies)

    fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(13, 5.6))
    _donut(ax0, [conf, nr], [C_TEAL, C_SIENNE],
           _pct(d["conformite_globale"]),
           "consignes conserver /\nétudier / ajouter",
           f"Conformes : {_n(conf)} / {_n(k['nb'])}  —  non retrouvés : {_n(nr)}")
    _donut(ax1, [del_ok, del_ko], [C_TEAL, C_ROUGE], _pct(c_del["pct"]),
           "consignes\n« à supprimer »",
           f"Encore au compte : {_n(del_ko)} dossiers — PM MRM {_meur(c_del['pm_mrm'])} non supprimée")
    handles = [
        Patch(color=C_TEAL,   label="Conforme"),
        Patch(color=C_SIENNE, label="Non retrouvé (absent du compte)"),
        Patch(color=C_ROUGE,  label="Encore au compte (à supprimer non suivie)"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=3, fontsize=F_LEG,
               frameon=False, columnspacing=1.6)
    _title(
        fig,
        f"Suivi des consignes : {_pct(d['conformite_globale'])} appliquées au compte — "
        f"suppression effective : {_pct(c_del['pct'])}",
        "Exercice courant (N+1 suivis à part) — gauche : conserver / étudier / ajouter "
        "(conforme = retrouvé) ; droite : à supprimer (conforme = absent)",
    )
    fig.subplots_adjust(top=0.80, bottom=0.16, left=0.04, right=0.96, wspace=0.25)
    return fig


# ============================================================================
# 9. PM REVUE vs PM COMPTE PAR CONSIGNE (Δ en € et en %)
# ============================================================================

def graph_pm_par_consigne(d: dict):
    """Pour chaque consigne : PM MRM et PM CPT côte à côte, le delta au-dessus."""
    consignes = [(c, v) for c, v in d["consignes"].items() if v["pertinent"]]
    x = list(range(len(consignes)))
    w = 0.34
    pm_mrm = [v["pm_mrm"] / 1e6 for _, v in consignes]
    pm_cpt = [v["pm_cpt"] / 1e6 for _, v in consignes]
    top = max(*pm_mrm, *pm_cpt) or 1.0

    fig, ax = plt.subplots(figsize=(12, 5.8))
    ax.bar([i - w / 2 for i in x], pm_mrm, width=w, color=C_BLEU,  label="PM revue MRM")
    ax.bar([i + w / 2 for i in x], pm_cpt, width=w, color=C_OCEAN, label="PM compte client")
    for i, (_, v) in enumerate(consignes):
        ax.text(i - w / 2, pm_mrm[i] + top * 0.015, _meur(v["pm_mrm"]),
                ha="center", va="bottom", fontsize=F_TXT - 1)
        ax.text(i + w / 2, pm_cpt[i] + top * 0.015, _meur(v["pm_cpt"]),
                ha="center", va="bottom", fontsize=F_TXT - 1)
        # Delta du groupe (en € et en %) — sienne si sous-provisionné, océan sinon.
        ax.text(i, max(pm_mrm[i], pm_cpt[i]) + top * 0.12,
                f"Δ {_meur(v['delta'])}  ({_pct(v['taux_chute'])})",
                ha="center", fontsize=F_TXT, fontweight="bold",
                color=C_SIENNE if v["delta"] > 0 else C_OCEAN)
    ax.set_xticks(x)
    ax.set_xticklabels([c for c, _ in consignes], fontsize=F_AXE)
    ax.set_ylim(0, top * 1.30)
    ax.legend(loc="upper right", fontsize=F_LEG, frameon=False)
    _style(ax, ylabel="PM (M€)")
    _title(
        fig,
        f"PM revue vs PM compte par consigne : Δ inventaire "
        f"{_meur(d['metrics_pm_ecart'])} "
        f"({_pct(d['taux_chute_inventaire'])})",
        "Δ = PM MRM − PM compte (positif = sous-provisionné) — matchés de l'inventaire "
        "courant, hors « à supprimer » / statut NON ; N+1 analysés à part",
    )
    fig.subplots_adjust(top=0.78, bottom=0.10, left=0.08, right=0.96)
    return fig


# ============================================================================
# 10. TAUX DE CHUTE PAR ANCIENNETÉ (année de survenance)
# ============================================================================

def _empty_fig(message: str):
    """Figure « aucune donnée » (garde-fou pour les axes ré-agrégés vides)."""
    fig, ax = plt.subplots(figsize=(8, 3))
    ax.axis("off")
    ax.text(0.5, 0.5, message, ha="center", va="center",
            fontsize=F_TITRE, color="#555555")
    return fig


def graph_chute_par_anciennete(pdf_anc, d: dict):
    """La méthode d'inventaire diffère selon l'année de survenance.

    pdf_anc = metrics.chute_par_anciennete(df_result, annee), bloc « Inventaire
    courant » (les stats globales) ; N+1 reste une analyse séparée."""
    pdf = pdf_anc[pdf_anc["EXERCICE"] == EXERCICE_INV].copy()
    if pdf.empty:
        return _empty_fig("Taux de chute par ancienneté : aucune donnée")
    labels = list(pdf["BLOC_ANCIENNETE"])
    taux   = list(pdf["TAUX_CHUTE_PCT"])
    colors = [C_SIENNE if t > 0 else C_OCEAN for t in taux]

    fig, ax = plt.subplots(figsize=(11, 5.6))
    ax.bar(labels, taux, color=colors, width=0.5)
    span = (max(taux) - min(taux)) or 1
    for i, (_, r) in enumerate(pdf.iterrows()):
        au_dessus = taux[i] >= 0
        ax.text(i, taux[i] + (span * 0.06 if au_dessus else -span * 0.06),
                f"{_pct(r['TAUX_CHUTE_PCT'])} — écart {_meur(r['ECART'])}, "
                f"poids {_pct(r['POIDS_PM_PCT'])}",
                ha="center", va="bottom" if au_dessus else "top", fontsize=F_TXT - 1)
    ax.axhline(0, color="#333333", linewidth=0.8, zorder=0.5)
    ax.axhline(d["taux_chute_inventaire"], color=C_GRIS, linewidth=1.4, linestyle="--",
               zorder=0.5,
               label=f"taux de chute inventaire : {_pct(d['taux_chute_inventaire'])}")
    ax.margins(y=0.26)
    ax.legend(loc="lower left", fontsize=F_LEG, frameon=False)
    _style(ax, ylabel="Taux de chute (%)")
    _title(
        fig,
        "Taux de chute par ancienneté : la méthode d'inventaire diffère selon "
        "l'année de survenance",
        "N / N-1 / N-2 et antérieur (revue tête par tête sur N-1) — matchés de "
        "l'inventaire courant, hors « à supprimer » / statut NON ; N+1 à part",
    )
    fig.subplots_adjust(top=0.78, bottom=0.10, left=0.10, right=0.96)
    return fig


# ============================================================================
# 11. ORPHELINS PAR COMPTE PB (investigation souscripteur)
# ============================================================================

def graph_orphelins_par_compte(pdf_orph, d: dict, top: int = 12):
    """Quel compte concentre le plus d'orphelins ? (à investiguer avec le
    souscripteur). pdf_orph = metrics.orphelins_par_clause(df_result) — table de
    détail : seuls les comptes portant une clause y figurent, les poids se
    lisent en part de TOUS les orphelins."""
    if pdf_orph.empty:
        return _empty_fig("Orphelins par compte : aucun orphelin porteur de clause")
    pdf = pdf_orph.head(top)[::-1]                       # plus gros volume en haut
    labels = [f"{c} ({t})" for c, t in zip(pdf["CLAUSE"], pdf["TYPE_COMPTE"])]
    colors = [C_ROUGE if r == 1 else C_OCEAN for r in pdf["RANG"]]

    h = 0.5 * len(pdf) + 3
    fig, ax = plt.subplots(figsize=(12, h))
    ax.barh(labels, pdf["NB_DOSSIERS"], color=colors, height=0.6)
    vmax = float(pdf["NB_DOSSIERS"].max()) or 1
    for i, (_, r) in enumerate(pdf.iterrows()):
        ax.text(r["NB_DOSSIERS"] + vmax * 0.015, i,
                f"{_n(r['NB_DOSSIERS'])} ({_pct(r['POIDS_NB_PCT'])}) — {_meur(r['PM_CPT'])}",
                va="center", fontsize=F_TXT - 1)
    ax.set_xlim(0, vmax * 1.45)
    _style(ax, xlabel="Nombre d'orphelins compte (dossiers sans contrepartie MRM)")
    top1 = pdf_orph.iloc[0]
    _title(
        fig,
        f"Orphelins par compte : le compte {top1['CLAUSE']} en concentre "
        f"{_n(top1['NB_DOSSIERS'])} ({_pct(top1['POIDS_NB_PCT'])})",
        f"Compte préposé le plus représentatif (RANG 1, en rouge) à investiguer "
        f"avec le souscripteur — {_n(d['def_nb'])} orphelins au total ({_meur(d['def_pm'])})",
    )
    fig.subplots_adjust(top=max(0.80, 1 - 1.3 / h), bottom=1.1 / h, left=0.22, right=0.97)
    return fig


# ============================================================================
# 12. DISTRIBUTION DES ÉCARTS DE PM (dossiers sur/sous-provisionnés par tranche)
# ============================================================================

def graph_distribution_ecarts(pdf_tranches, d: dict):
    """Combien de dossiers sur/sous-provisionnés, et à quelle ampleur ?

    Le taux agrégé dit le solde ; la distribution dit combien de dossiers
    portent un écart et à quel niveau (un taux quasi nul peut cacher de gros
    écarts compensés). pdf_tranches = metrics.chute_par_tranche_ecart
    (df_result), bloc « Inventaire courant » (les stats globales)."""
    pdf = pdf_tranches[pdf_tranches["EXERCICE"] == EXERCICE_INV].copy()
    if pdf.empty or pdf["NB_DOSSIERS"].sum() == 0:
        return _empty_fig("Distribution des écarts : aucune donnée")

    ordre_nul = int(pdf.loc[pdf["TRANCHE_ECART"] == TRANCHE_ECART_NUL, "ORDRE"].iloc[0])
    colors = [C_OCEAN if o < ordre_nul else C_GRIS if o == ordre_nul else C_SIENNE
              for o in pdf["ORDRE"]]

    fig, ax = plt.subplots(figsize=(12.5, 5.8))
    ax.bar(pdf["TRANCHE_ECART"], pdf["NB_DOSSIERS"], color=colors, width=0.62)
    vmax = float(pdf["NB_DOSSIERS"].max()) or 1
    for i, (_, r) in enumerate(pdf.iterrows()):
        if r["NB_DOSSIERS"]:
            ax.text(i, r["NB_DOSSIERS"] + vmax * 0.02, _n(r["NB_DOSSIERS"]),
                    ha="center", va="bottom", fontsize=F_TXT - 1)
    ax.margins(y=0.14)
    ax.set_xticks(range(len(pdf)))
    ax.set_xticklabels(pdf["TRANCHE_ECART"], rotation=30, ha="right")
    ax.legend(handles=[
        Patch(color=C_OCEAN,  label="sur-provisionné (marge)"),
        Patch(color=C_GRIS,   label="écart nul"),
        Patch(color=C_SIENNE, label="sous-provisionné (risque)"),
    ], loc="upper right", fontsize=F_LEG, frameon=False)
    _style(ax, ylabel="Nombre de dossiers")

    sous = int(pdf["NB_SOUS_PROVISION"].sum())
    sur  = int(pdf["NB_SUR_PROVISION"].sum())
    nul  = int(pdf["NB_ECART_NUL"].sum())
    _title(
        fig,
        f"Écarts de provision dossier par dossier : {_n(sous)} sous-provisionnés, "
        f"{_n(sur)} sur-provisionnés, {_n(nul)} à l'équilibre",
        "Écart signé (PM revue − PM compte) par tranche de seuils — matchés de "
        f"l'inventaire courant, hors « à supprimer » / statut NON ; solde "
        f"{_meur(d['metrics_pm_ecart'])} ({_pct(d['taux_chute_inventaire'])})",
    )
    fig.subplots_adjust(top=0.80, bottom=0.24, left=0.08, right=0.97)
    return fig


# ============================================================================
# ORCHESTRATEUR
# ============================================================================

def restituer_graphiques(
    df_result: DataFrame,
    d        : dict = None,
    save_dir : str = GRAPHS_DIR_DEFAULT,
    show     : bool = True,
    top      : int = 12,
) -> dict:
    """
    Construit les 12 graphiques de restitution, les affiche (notebook) et les
    écrit en PNG (save_dir, DBFS). save_dir=None → pas d'écriture.

    `d` = dict de compute_synthese si déjà calculé (ex. retour de
    print_synthese) — sinon la passe Spark est lancée ici.

    Returns:
        dict {nom: Figure} — réutilisable (insertion Excel/PowerPoint).
    """
    d = d if d is not None else compute_synthese(df_result)
    annee = _annee_inventaire(d)

    figs = {
        "1_compte_justification"   : graph_compte_justification(d),
        "2_couverture_mrm"         : graph_couverture_mrm(d),
        "3_chute_par_type_compte"  : graph_chute_par_type_compte(chute_par_type_compte(df_result), d),
        "4_chute_par_consigne"     : graph_chute_par_consigne(d),
        "5_conformite_consignes"   : graph_conformite_consignes(d),
        "6_anomalies_cpt_only"     : graph_anomalies_cpt_only(anomalies_cpt_only(df_result), d),
        "7_kpi_chute"              : graph_kpi_chute(d),
        "8_kpi_conformite_globale" : graph_kpi_conformite_globale(d),
        "9_pm_par_consigne"        : graph_pm_par_consigne(d),
        "10_chute_par_anciennete"  : graph_chute_par_anciennete(chute_par_anciennete(df_result, annee), d),
        "11_orphelins_par_compte"  : graph_orphelins_par_compte(orphelins_par_clause(df_result), d),
        "12_distribution_ecarts"   : graph_distribution_ecarts(chute_par_tranche_ecart(df_result), d),
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
