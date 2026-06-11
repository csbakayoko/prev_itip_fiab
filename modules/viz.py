"""
Restitution graphique des métriques (matplotlib) — titres porteurs de message.

Consomme exclusivement la couche métriques (modules.metrics.Metrics) : viz ne
calcule rien, elle met en forme. Chaque graphique répond à une question de la
problématique de fiabilisation (direction financière et engagements) :
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
    9. pm_par_consigne        — PM revue vs PM compte par consigne (Δ en € et en %)

Usage (notebook Databricks) :
    from modules.metrics import Metrics
    from modules.viz import restituer_graphiques

    m = Metrics(df_result)                            # une passe Spark
    figs = restituer_graphiques(m)                    # affiche + PNG DBFS
    figs = restituer_graphiques(m, save_dir=None)     # affiche seulement
"""

import os

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from pyspark.sql import DataFrame

from modules.metrics import Metrics, output_dir, _to_local

# Palette AXA en priorité, complétée quand la sémantique l'exige.
C_BLEU   = "#00008F"   # AXA Blue   — référence (revue MRM, matchés)
C_OCEAN  = "#4976BA"   # AXA Ocean  — compte / récupérés N+1 / sur-provisionné
C_TEAL   = "#027180"   # AXA Teal   — conforme / couvert
C_SIENNE = "#F07662"   # AXA Sienna — sous-provisionné, à étudier (risque modéré)
C_ROUGE  = "#FF1721"   # AXA Red    — anomalies / non conforme (alerte)
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

def graph_compte_justification(m: Metrics):
    """Le compte client est-il justifié par la revue d'inventaire ?"""
    d = m.d
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

def graph_couverture_mrm(m: Metrics):
    """Quelle part de la revue MRM est retrouvée au compte ? (+ « à supprimer »
    retrouvées : consignes de suppression non suivies)."""
    d = m.d
    base  = d["a_comparer_nb"] or 1
    c_del = d["consignes"]["À supprimer"]
    del_ko = c_del["nb"] - c_del["conf"]          # retrouvées alors qu'à supprimer
    pct = lambda nb, den: round(nb / (den or 1) * 100, 1)

    bars = [
        # (libellé, nb, pct affiché, PM MRM, couleur, hachures)
        ("Retrouvés au compte",      d["match_nb"], pct(d["match_nb"], base), None,          C_TEAL,   None),
        ("À conserver non retrouvé (non conforme)", d["keep_nb"],  pct(d["keep_nb"],  base), d["keep_pm"],  C_ROUGE,  None),
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

def graph_chute_par_clause(m: Metrics, top: int = 12):
    """Quelles clauses portent l'écart de provisionnement ?"""
    d, k = m.d, m.k
    pdf = m.chute_par_clause(top=top).data[::-1]
    labels = [f"{c} ({t})" for c, t in zip(pdf["CLAUSE"], pdf["TYPE_CLAUSE"])]
    colors = [C_SIENNE if v > 0 else C_OCEAN for v in pdf["taux_chute_pct"]]

    h = 0.6 * len(pdf) + 3.2
    fig, ax = plt.subplots(figsize=(12, h))
    ax.barh(labels, pdf["taux_chute_pct"], color=colors, height=0.55)
    for i, (v, e, p) in enumerate(zip(pdf["taux_chute_pct"], pdf["ecart_signe"], pdf["poids_pm_pct"])):
        ax.text(v + (0.5 if v >= 0 else -0.5), i,
                f"{_pct(v)}   (écart {_meur(e)}, poids {_pct(p)})",
                va="center", ha="left" if v >= 0 else "right", fontsize=F_TXT - 1)
    ax.axvline(0, color="#333333", linewidth=0.8, zorder=0.5)
    ax.axvline(d["taux_chute_global"], color=C_GRIS, linewidth=1.4, linestyle="--",
               zorder=0.5, label=f"taux de chute global : {_pct(d['taux_chute_global'])}")
    ax.legend(loc="lower right", fontsize=F_LEG, frameon=False)
    lo = min(float(pdf["taux_chute_pct"].min()), 0)
    hi = max(float(pdf["taux_chute_pct"].max()), 0)
    ax.set_xlim(lo - (hi - lo) * 0.65 - 2, hi + (hi - lo) * 0.65 + 2)
    _style(ax, xlabel="Taux de chute (%) — positif = sous-provisionné (risque), négatif = sur-provisionné")
    _title(
        fig,
        f"Provisionnement par clause : taux de chute global {_pct(d['taux_chute_global'])} "
        f"(écart {_meur(k['delta'])})",
        f"Top {len(pdf)} clauses par PM MRM — dossiers retrouvés au compte (inventaire + N+1), "
        f"consignes conserver / étudier / ajouter",
    )
    fig.subplots_adjust(top=max(0.80, 1 - 1.3 / h), bottom=1.1 / h, left=0.18, right=0.97)
    return fig


# ============================================================================
# 4. TAUX DE CHUTE PAR CONSIGNE
# ============================================================================

def graph_chute_par_consigne(m: Metrics):
    """L'écart de provision selon la consigne posée par la revue."""
    d = m.d
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
    ax.axhline(d["taux_chute_global"], color=C_GRIS, linewidth=1.4, linestyle="--",
               zorder=0.5, label=f"taux de chute global : {_pct(d['taux_chute_global'])}")
    ax.margins(y=0.26)
    ax.legend(loc="lower left", fontsize=F_LEG, frameon=False)
    _style(ax, ylabel="Taux de chute (%)")
    _title(
        fig,
        "Le taux de chute global est la moyenne pondérée (par la PM MRM) des consignes",
        "Positif = sous-provisionné (risque) — dossiers retrouvés au compte (inventaire + N+1), "
        "« à supprimer » suivie à part",
    )
    fig.subplots_adjust(top=0.78, bottom=0.10, left=0.10, right=0.96)
    return fig


# ============================================================================
# 5. CONFORMITÉ DES CONSIGNES (toutes les consignes, « à supprimer » incluse)
# ============================================================================

def graph_conformite_consignes(m: Metrics):
    """Les consignes de la revue sont-elles appliquées au compte ?

    Le reste-à-100 % est qualifié selon la consigne (3 états) : « non conforme »
    pour KEEP absent / à supprimer encore présent (anomalie), « non retrouvé »
    pour à ajouter / à étudier absents (informatif)."""
    d = m.d
    items = list(d["consignes"].items())[::-1]
    labels = [c for c, _ in items]
    conf   = [v["pct"] for _, v in items]
    nonc   = [round(100 - v["pct"], 1) for _, v in items]
    # Couleur du KO selon sa nature : sienne = non retrouvé, rouge = non conforme.
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
        Patch(color=C_ROUGE,  label="Non conforme (KEEP absent / à supprimer présent)"),
        Patch(color=C_SIENNE, label="Non retrouvé (à ajouter / à étudier absent)"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=3, fontsize=F_LEG,
               frameon=False, columnspacing=1.6)
    _style(ax, xlabel="Part des dossiers conformes (%)")
    _title(
        fig,
        f"Application des consignes de la revue : {_pct(d['conformite_globale'])} de "
        f"conformité globale (hors « à supprimer »)",
        "Conserver : conforme = retrouvé — ajouter / étudier : retrouvé ou « non "
        "retrouvé » — à supprimer : conforme = absent du compte",
    )
    fig.subplots_adjust(top=0.78, bottom=0.26, left=0.15, right=0.97)
    return fig


# ============================================================================
# 6. ANOMALIES RÉSIDUELLES CPT_ONLY (saisonnalité)
# ============================================================================

def graph_anomalies_cpt_only(m: Metrics):
    """Volume / PM des anomalies par mois de survenance — effet fin d'année."""
    d = m.d
    pdf = m.anomalies_cpt_only().data
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
# 7. KPI — TAUX DE CHUTE GLOBAL
# ============================================================================

def graph_kpi_chute_globale(m: Metrics):
    """Le ratio de chute global en un visuel : gros chiffre + PM en regard."""
    d, k = m.d, m.k
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

    bars = [("PM revue MRM", k["pm_mrm"], C_BLEU), ("PM compte client", k["pm_cpt"], C_OCEAN)]
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
        "Σ(PM MRM − PM CPT) / Σ PM MRM — dossiers retrouvés au compte (inventaire + N+1), "
        "consignes conserver / étudier / ajouter",
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


def graph_kpi_conformite_globale(m: Metrics):
    """Suivi des consignes au global : conformité KAS + suppression effective.

    Donut gauche en 3 parts (conforme / non retrouvé / non conforme) : pour les
    consignes conserver/étudier/ajouter, le « non retrouvé » (à ajouter/étudier
    absents) est distingué du « non conforme » (KEEP absent)."""
    d, k = m.d, m.k
    cons = d["consignes"]
    conf = k["conf"]
    nr   = cons["À ajouter"]["ko"] + cons["À étudier"]["ko"]   # non retrouvés
    nc   = cons["À conserver"]["ko"]                           # non conformes (KEEP)
    c_del  = d["consignes"]["À supprimer"]
    del_ok = c_del["conf"]                      # effectivement supprimées
    del_ko = c_del["nb"] - del_ok               # retrouvées au compte (non suivies)

    fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(13, 5.6))
    _donut(ax0, [conf, nr, nc], [C_TEAL, C_SIENNE, C_ROUGE],
           _pct(d["conformite_globale"]),
           "consignes conserver /\nétudier / ajouter",
           f"Conformes : {_n(conf)} / {_n(k['nb'])}  —  "
           f"non retrouvés : {_n(nr)}  —  non conformes : {_n(nc)}")
    _donut(ax1, [del_ok, del_ko], [C_TEAL, C_ROUGE], _pct(c_del["pct"]),
           "consignes\n« à supprimer »",
           f"Encore au compte : {_n(del_ko)} dossiers — PM MRM {_meur(c_del['pm_mrm'])} non supprimée")
    handles = [
        Patch(color=C_TEAL,   label="Conforme"),
        Patch(color=C_SIENNE, label="Non retrouvé (à ajouter / étudier)"),
        Patch(color=C_ROUGE,  label="Non conforme"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=3, fontsize=F_LEG,
               frameon=False, columnspacing=1.6)
    _title(
        fig,
        f"Suivi des consignes : {_pct(d['conformite_globale'])} appliquées au compte — "
        f"suppression effective : {_pct(c_del['pct'])}",
        "Gauche : conserver / étudier / ajouter (conforme = retrouvé au compte) — "
        "droite : à supprimer (conforme = absent du compte)",
    )
    fig.subplots_adjust(top=0.80, bottom=0.16, left=0.04, right=0.96, wspace=0.25)
    return fig


# ============================================================================
# 9. PM REVUE vs PM COMPTE PAR CONSIGNE (Δ en € et en %)
# ============================================================================

def graph_pm_par_consigne(m: Metrics):
    """Pour chaque consigne : PM MRM et PM CPT côte à côte, le delta au-dessus."""
    d, k = m.d, m.k
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
        f"PM revue vs PM compte par consigne : Δ global {_meur(k['delta'])} "
        f"({_pct(d['taux_chute_global'])})",
        "Δ = PM MRM − PM compte (positif = sous-provisionné) — "
        "dossiers retrouvés au compte (inventaire + N+1)",
    )
    fig.subplots_adjust(top=0.78, bottom=0.10, left=0.08, right=0.96)
    return fig


# ============================================================================
# ORCHESTRATEUR
# ============================================================================

def restituer_graphiques(
    metrics  : "Metrics | DataFrame",
    save_dir : str = GRAPHS_DIR_DEFAULT,
    show     : bool = True,
) -> dict:
    """
    Construit les 9 graphiques de restitution, les affiche (notebook) et les
    écrit en PNG (save_dir, DBFS). save_dir=None → pas d'écriture.

    Accepte un objet Metrics (passe Spark déjà faite, à privilégier) ou un
    df_result brut (la passe est alors lancée ici).

    Returns:
        dict {nom: Figure} — réutilisable (insertion Excel/PowerPoint).
    """
    m = metrics if isinstance(metrics, Metrics) else Metrics(metrics)

    figs = {
        "1_compte_justification"   : graph_compte_justification(m),
        "2_couverture_mrm"         : graph_couverture_mrm(m),
        "3_chute_par_clause"       : graph_chute_par_clause(m),
        "4_chute_par_consigne"     : graph_chute_par_consigne(m),
        "5_conformite_consignes"   : graph_conformite_consignes(m),
        "6_anomalies_cpt_only"     : graph_anomalies_cpt_only(m),
        "7_kpi_chute_globale"      : graph_kpi_chute_globale(m),
        "8_kpi_conformite_globale" : graph_kpi_conformite_globale(m),
        "9_pm_par_consigne"        : graph_pm_par_consigne(m),
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
