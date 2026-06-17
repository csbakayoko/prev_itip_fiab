"""
Rendu ASCII de la synthèse (boîte 3 bulles + indicateurs + suivi consignes).

PUR : formatage de chaînes depuis le dict de la synthèse, AUCUNE dépendance
Spark ni pandas. Séparé du calcul (kpi_export / synthese_scalars) pour que le
module « source de vérité » ne fasse plus de présentation. print_synthese
(kpi_export) orchestre calcul + rendu.
"""

from config import CLIENT_NAME


_B    = 34         # largeur d'une bulle
_LBL  = 27         # largeur du libellé dans la colonne latérale
_NBW  = 7          # largeur du nombre de dossiers (jusqu'à 9 999 999)
_PMW  = 13         # largeur de la PM (jusqu'aux milliards : "1 554 072 064")
_RW   = _LBL + 2 + _NBW + 2 + _PMW + 2   # libellé + ": " + nb + "  " + pm + " €"
_T    = 3 + _B + 3 + _RW   # largeur du contenu intérieur de la boîte


def _n(x) -> str:
    """Entier avec séparateur de milliers espace (style FR)."""
    return f"{int(round(x or 0)):,}".replace(",", " ")


def _row(label: str, nb, pm) -> str:
    """Ligne latérale : 'libellé : nb  PM €' (nb et PM alignés à droite).

    pm=None → colonne PM laissée vide (sous-total sans contrepartie PM pertinente).
    """
    pm_txt = " " * (_PMW + 2) if pm is None else f"{_n(pm):>{_PMW}} €"
    return f"{label:<{_LBL}}: {_n(nb):>{_NBW}}  {pm_txt}"


def _bubble(*lines: str) -> list:
    """Construit une mini-bulle encadrée de largeur _B."""
    inner = _B - 4
    top = "┌" + "─" * (_B - 2) + "┐"
    bot = "└" + "─" * (_B - 2) + "┘"
    body = ["│ " + ln[:inner].ljust(inner) + " │" for ln in lines]
    return [top, *body, bot]


def _render_box(d: dict, client: str) -> str:
    """Bloc principal en 3 bulles + colonne latérale détaillée."""
    left = (
        _bubble(
            f"MRM = {_n(d['mrm_nb'])} dossiers",
            f"     / PM {_n(d['mrm_pm'])} €",
        )
        + _bubble(
            f"RETROUVÉS = {_n(d['trouves_nb'])} dossiers",
            f" {_n(d['match_nb'])} inv. + {_n(d['late_nb'])} N+1",
            f" PM MRM   {_n(d['trouves_pm_mrm'])} €",
            f" PM CPT   {_n(d['trouves_pm_cpt'])} €",
            f" Δ PM     {_n(d['trouves_pm_mrm'] - d['trouves_pm_cpt'])} €",
        )
        + _bubble(
            f"COMPTE = {_n(d['cpt_nb'])} dossiers",
            f"      / PM {_n(d['cpt_pm'])} €",
        )
    )

    # « À supprimer » encore au compte (matchés DELETE) = sous-ensemble des
    # retrouvés, consigne non suivie. Affiché pour réconcilier avec la table consignes :
    #   à supprimer (absents=OK) + encore au compte (KO) = total consigne à supprimer.
    del_ko = d["consignes"]["À supprimer"]["ko"]
    right = [
        "",
        _row("À supprimer — absents (OK)", d["a_supprimer_nb"], d["a_supprimer_pm"]),
        _row("À comparer",            d["a_comparer_nb"],  d["a_comparer_pm"]),
        _row("Retrouvés clé principale", d["principale_nb"], d["principale_pm"]),
        _row("Retrouvés clé affinée",    d["affinee_nb"],    d["affinee_pm_mrm"]),
        _row("Retrouvés récupération",   d["recup_nb"],      d["recup_pm_mrm"]),
        _row("Retrouvés clé clause",     d["clause_nb"],     d["clause_pm"]),
        _row("└ dont à supprimer (KO)",  del_ko,             None),
        _row("Non retrouvés au compte",  d["non_mappes_nb"], d["non_mappes_pm"]),
        _row("├ à conserver",         d["keep_nb"],        d["keep_pm"]),
        _row("├ à étudier",           d["study_nb"],       d["study_pm"]),
        _row("└ à ajouter",           d["add_nb"],         d["add_pm"]),
        "",
        _row("Total CPT (compte)",          d["cpt_nb"],     d["cpt_pm"]),
        _row("├ retrouvés (inventaire)",    d["match_nb"],   d["match_pm_cpt"]),
        _row("├ retrouvés via N+1",         d["late_nb"],    d["late_pm"]),
        _row("├ repêchés (statut MRM non)", d["recup_non_nb"], d["recup_non_pm"]),
        _row("├ clos avant inv. N+1",       d["obs_nb"],     d["obs_pm"]),
        _row("└ sans contrepartie (anom.)", d["def_nb"],     d["def_pm"]),
        "",
    ]

    top    = "┌" + "─" * _T + "┐"
    sep    = "├" + "─" * _T + "┤"
    bottom = "└" + "─" * _T + "┘"
    header = f"  Compte client : {client}".ljust(38) + f"Date inventaire : {d['date_inventaire']}"

    out = [top, "│" + header.ljust(_T) + "│", sep]
    for i in range(max(len(left), len(right))):
        l = left[i]  if i < len(left)  else " " * _B
        r = right[i] if i < len(right) else ""
        content = "   " + l + "   " + r
        out.append("│" + content.ljust(_T) + "│")
    out.append(bottom)
    return "\n".join(out)


def _render_indicateurs(d: dict) -> str:
    """Bloc des taux globaux + niveaux de PM (univers : matchés légitimes + tardifs)."""
    coh = "✔ cohérent" if d["coherent"] else "✘ INCOHÉRENT"
    detail_coh = (
        f"{_n(d['classified_rows'])} / {_n(d['total_rows'])} lignes classées"
        + ("" if d["coherent"]
           else f" — labels non pris en compte : {', '.join(d['labels_inconnus']) or 'n/d'}")
    )
    lines = [
        "LEXIQUE : retrouvé = dossier de la revue présent au compte | non retrouvé = absent du compte",
        "          conforme = consigne respectée (conserver/étudier/ajouter → retrouvé ; à supprimer → absent)",
        "",
        "INDICATEURS",
        "  COUVERTURE",
        f"    Taux de couverture MRM (retrouvés / à comparer)        : {d['taux_couverture_mrm']:>5} %",
        f"    Taux de couverture compte (retrouvés inv. / compte)   : {d['taux_couverture_compte']:>5} %",
        "  RÉCUPÉRATION (compte, déclarations tardives N+1)",
        f"    Taux de récupération tardive (retrouvés N+1 / restes) : {d['taux_recup_tardive']:>5} %",
        f"    Taux de récupération global (retrouvés + N+1 / compte): {d['taux_recup_global']:>5} %",
        "  PROVISIONNEMENT",
        f"    Taux de chute (inventaire courant, hors suppr. / NON)  : {d['taux_chute_inventaire']:>5} %",
        f"      ↳ contrôle Σ consignes : {d['taux_chute_consignes']:>5} %  "
        + ("✔ cohérent" if d["chute_coherente"] else "✘ INCOHÉRENT (voir logs)"),
        f"    Taux de chute récupérés N+1 (analyse séparée)          : {d['taux_chute_n1']:>5} %",
        f"    Conformité globale des consignes                       : {d['conformite_globale']:>5} %",
        "  (dénominateurs compte hors sinistres clos avant inventaire suivant)",
        "",
        f"NIVEAUX DE PM — base du taux de chute ({_n(d['metrics_nb'])} dossiers :",
        "  matchés de l'inventaire courant, hors « à supprimer » et hors statut NON —",
        "  récupérés N+1 et repêchés statut NON analysés à part)",
        f"  PM MRM   : {_n(d['metrics_pm_mrm']):>15} €",
        f"  PM CPT   : {_n(d['metrics_pm_cpt']):>15} €",
        f"  Écart    : {_n(d['metrics_pm_ecart']):>15} €",
        f"  (dont {_n(d['hors_consigne_nb'])} dossiers sans consigne reconnue — "
        f"PM MRM {_n(d['hors_consigne_pm_mrm'])} €, inclus dans la base)",
        "",
        f"RÉCUPÉRATION TARDIVE N+1 ({_n(d['late_nb'])} dossiers — analyse séparée, "
        f"HORS stats globales)",
        f"  Dossiers CPT orphelins retrouvés dans l'inventaire N+1  "
        f"(PM MRM {_n(d['late_pm_mrm'])} € | PM CPT {_n(d['late_pm_cpt'])} €)",
        f"  Taux de chute N+1 : {d['taux_chute_n1']} %  (base {_n(d['chute_n1_nb'])} "
        f"hors « à supprimer », PM MRM {_n(d['chute_n1_pm_mrm'])} € | "
        f"PM CPT {_n(d['chute_n1_pm_cpt'])} €)",
        f"  Consignes N+1 : conserver {_n(d['n1_consignes']['À conserver'])} · "
        f"étudier {_n(d['n1_consignes']['À étudier'])} · "
        f"ajouter {_n(d['n1_consignes']['À ajouter'])} (conformes) · "
        f"à supprimer encore au compte {_n(d['n1_consignes']['À supprimer'])}"
        + (f" · sans consigne {_n(d['n1_sans_consigne'])}" if d["n1_sans_consigne"] else ""),
        "",
        f"SINISTRES CLOS AVANT INVENTAIRE SUIVANT ({_n(d['obs_nb'])} dossiers, hors métriques)",
        "  Obs. tardives IT (garantie 60, fin d'année) : sinistre clos avant l'inventaire",
        f"  MRM N+1 → non retrouvé (explicable, pas une anomalie). PM CPT {_n(d['obs_pm'])} €.",
        "",
        f"RÉCUPÉRÉS VIA MRM STATUT NON ({_n(d['recup_non_nb'])} dossiers, hors métriques)",
        "  CPT_ONLY repêchés sur un MRM statut NON (PM MRM = 0, non remonté à la",
        f"  direction financière) : anomalie résolue. PM CPT {_n(d['recup_non_pm'])} €. Voir analyse dédiée.",
        f"  ↳ part par exercice : N {_n(d['recup_non_n_nb'])} (PM CPT {_n(d['recup_non_n_pm'])} €) · "
        f"N+1 {_n(d['recup_non_n1_nb'])} (PM CPT {_n(d['recup_non_n1_pm'])} €)",
        "  ↳ contrôle PM MRM = 0 : "
        + ("✔ vérifié" if d["recup_non_pm_mrm_ok"]
           else f"✘ VIOLÉ — {_n(d['recup_non_pm_mrm_nz'])} dossier(s), "
                f"PM MRM {_n(d['recup_non_pm_mrm'])} € (voir logs)"),
        "",
        f"ANOMALIES — CPT_ONLY définitifs ({_n(d['def_nb'])} dossiers, PM CPT {_n(d['def_pm'])} €)",
        "  Dossiers compte sans contrepartie MRM, ni récupérés, ni explicables.",
        "",
        f"CONTRÔLE DE COHÉRENCE : {coh} — {detail_coh}",
    ]
    return "\n".join(lines)


def _np(n, p) -> str:
    """Formate 'n (p%)' — volumétrie avec pourcentage entre parenthèses."""
    return f"{_n(n)} ({p}%)"


def _render_consignes(d: dict) -> str:
    """
    Suivi des consignes de l'EXERCICE COURANT — deux lectures réconciliables :

      CONFORMITÉ : total = retrouvés + reste ; %conf = conformes / total ;
        reste = non retrouvé (conserver/étudier/ajouter absents du compte)
        ou encore au compte (à supprimer non suivie).
      PROVISIONNEMENT : base = matchés de l'inventaire courant servant à la
        PM et au taux de chute ; PM MRM, PM CPT, chute. "À supprimer" → PM
        non pertinente.

    Les récupérés N+1 n'apparaissent pas ici (consigne d'un autre exercice) :
    leur suivi et leur chute sont dans le bloc RÉCUPÉRATION TARDIVE N+1.
    Pour conserver/étudier/ajouter, conformes == base (retrouvés inventaire).
    """
    head = (f"  {'Consigne':<13}│{'total':>7}{'conformes':>11}{'%conf':>8}"
            f"{'reste (statut)':>22}  │{'base':>7}"
            f"{'PM MRM':>16}{'PM CPT':>16}{'chute':>8}")
    sep  = "  " + "─" * (len(head) - 2)
    lines = [
        "SUIVI DES CONSIGNES — EXERCICE COURANT (les N+1 ont leur suivi séparé, cf. bloc N+1)",
        "  CONFORMITÉ : retrouvés à l'inventaire courant vs non retrouvés — conformes / total.",
        "  PROVISIONNEMENT : PM & taux de chute des matchés de l'inventaire courant.",
        "  Reste : conserver/étudier/ajouter = non retrouvé (absent du compte) ; à supprimer = encore au compte.",
        head,
        sep,
    ]
    for label, c in d["consignes"].items():
        statut = f"{_n(c['ko'])} {c['ko_label']}" if c["ko"] else "—"
        left = (f"  {label:<13}│{_n(c['nb']):>7}{_n(c['conf']):>11}{c['pct']:>6} %"
                f"{statut:>22}  │")
        if not c["pertinent"]:
            lines.append(left + f"{_n(c['nb_match']):>7}"
                         + "      — PM non pertinente (à supprimer) —")
        else:
            lines.append(
                left
                + f"{_n(c['nb_match']):>7}"
                + f"{_n(c['pm_mrm']):>14} €{_n(c['pm_cpt']):>14} €{c['taux_chute']:>6} %"
            )
    return "\n".join(lines)


def render_synthese(d: dict, client: str = CLIENT_NAME) -> str:
    """Rend la synthèse complète : box + indicateurs + consignes."""
    return "\n\n".join([
        _render_box(d, client),
        _render_indicateurs(d),
        _render_consignes(d),
    ])
