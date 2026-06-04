"""
Synthèse client — vue compacte type "schéma SODEXO".

Produit à chaque run :
  1. Une vue d'ensemble en 3 bulles (MRM / MATCHÉS / COMPTE) avec, pour chaque
     sous-catégorie, la volumétrie en nombre de dossiers et en PM (€).
  2. Un bloc d'INDICATEURS (taux de couverture, récupération tardive, taux de
     chute global, niveaux de PM).
  3. Un bloc SUIVI DES CONSIGNES (taux de conformité par consigne, avec la
     volumétrie des dossiers à PM non nulle).

Décodage des grandeurs (depuis df_result + TYPE_RECONCILIATION) :

    MRM      = MATCHÉS + à supprimer + non mappés (total dossiers MRM en entrée)
    MATCHÉS  = clé principale (EXACT + WINDOW) + clé affinée (TRONC + TRONC_WINDOW)
               + récupération (IP / rechute / rechute tronquée)
    COMPTE   = MATCHÉS + récupérés tardifs (CPT_LATE) + CPT_ONLY définitifs

    PM : côté MRM (MRM_PM) pour les ventilations MRM, côté CPT (CPT_PM) pour les CPT.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from config import (
    CLIENT_NAME,
    DATE_INVENTAIRE,
    MATCH_LABELS,
    MATCH_PRINCIPALE,
    MATCH_AFFINEE,
    MATCH_RECUPERATION,
)
from modules.matching import categorize_mrm_conclusion
from modules._timing import timed_fn


_KAS = ("MRM_KEEP", "MRM_ADD", "MRM_STUDY")   # consignes "à comparer" (hors DELETE)


def _pct(num, den) -> float:
    """Pourcentage arrondi à 0.1, 0.0 si dénominateur nul."""
    return round(num / den * 100, 1) if den else 0.0


# ============================================================================
# CALCUL DES SCALAIRES (une seule passe Spark)
# ============================================================================

def compute_synthese(df_result: DataFrame) -> dict:
    """
    Agrège df_result en une passe (nb + PM MRM + PM CPT + volumétrie PM≠0 par
    catégorie) et retourne les scalaires de la synthèse.

    Colonnes attendues : TYPE_RECONCILIATION, MRM_PM, CPT_PM, MRM_CONCLUSION.
    """
    df = df_result.withColumn(
        "MRM_ACTION", categorize_mrm_conclusion(F.col("MRM_CONCLUSION"))
    )
    # LATE_SOURCE absent si aucune récupération tardive n'a tourné → colonne neutre.
    if "LATE_SOURCE" not in df.columns:
        df = df.withColumn("LATE_SOURCE", F.lit(None).cast("string"))

    rows = (
        df.groupBy("TYPE_RECONCILIATION", "MRM_ACTION", "LATE_SOURCE")
        .agg(
            F.count("*").alias("nb"),
            F.coalesce(F.sum("MRM_PM"), F.lit(0.0)).alias("pm_mrm"),
            F.coalesce(F.sum("CPT_PM"), F.lit(0.0)).alias("pm_cpt"),
            # Volumétrie des dossiers dont la PM est non nulle (non-null ET ≠ 0)
            F.sum(F.when(F.col("MRM_PM").isNotNull() & (F.col("MRM_PM") != 0), 1).otherwise(0)).alias("nb_pm_mrm_nz"),
            F.sum(F.when(F.col("CPT_PM").isNotNull() & (F.col("CPT_PM") != 0), 1).otherwise(0)).alias("nb_pm_cpt_nz"),
        )
        .collect()
    )

    princ    = set(MATCH_PRINCIPALE)
    aff      = set(MATCH_AFFINEE)
    recup    = set(MATCH_RECUPERATION)
    match    = set(MATCH_LABELS)       # matchs LÉGITIMES de l'inventaire courant
    T = lambda r: r["TYPE_RECONCILIATION"]
    A = lambda r: r["MRM_ACTION"]
    S = lambda r: r["LATE_SOURCE"]

    def agg(field, pred):
        return sum(r[field] for r in rows if pred(r))

    def mrm(pred):   # → (nb, pm_mrm)
        return agg("nb", pred), agg("pm_mrm", pred)

    def cpt(pred):   # → (nb, pm_cpt)
        return agg("nb", pred), agg("pm_cpt", pred)

    nb_princ, pm_princ      = mrm(lambda r: T(r) in princ)
    nb_aff,   pm_aff_mrm    = mrm(lambda r: T(r) in aff)
    nb_recup, pm_recup_mrm  = mrm(lambda r: T(r) in recup)
    nb_del,   pm_del        = mrm(lambda r: T(r) == "MRM_DELETE")
    nb_miss,  pm_miss       = mrm(lambda r: T(r) == "MRM_MISSING")
    nb_def,   pm_def        = cpt(lambda r: T(r) == "CPT_ONLY")
    nb_late,  pm_late_cpt   = cpt(lambda r: T(r) == "CPT_LATE")
    pm_late_mrm             = agg("pm_mrm", lambda r: T(r) == "CPT_LATE")

    # Ventilation des CPT_LATE par origine : retrouvés dans un inventaire ultérieur
    # (LATE_SOURCE commençant par "MRM_") vs observations tardives IT heuristiques
    # (LATE_SOURCE == "OBS_TARDIVE_IT", sans contrepartie MRM).
    is_late      = lambda r: T(r) == "CPT_LATE"
    is_late_n1   = lambda r: is_late(r) and (S(r) or "").startswith("MRM_")
    is_late_obs  = lambda r: is_late(r) and not (S(r) or "").startswith("MRM_")
    nb_late_n1   = agg("nb",     is_late_n1)
    nb_late_obs  = agg("nb",     is_late_obs)
    pm_late_obs  = agg("pm_cpt", is_late_obs)

    # Matchés légitimes de l'inventaire courant.
    nb_match     = nb_princ + nb_aff + nb_recup
    pm_match_mrm = agg("pm_mrm", lambda r: T(r) in match)
    pm_match_cpt = agg("pm_cpt", lambda r: T(r) in match)

    # Univers MÉTRIQUES PM = matchés légitimes + déclarations tardives (CPT_LATE,
    # toutes origines : retrouvées en N+1 ou observations tardives IT).
    in_metrics     = lambda r: T(r) in match or T(r) == "CPT_LATE"
    pm_metrics_mrm = pm_match_mrm + pm_late_mrm
    pm_metrics_cpt = pm_match_cpt + pm_late_cpt
    nb_metrics     = nb_match + nb_late

    # Totaux exhaustifs des deux univers d'entrée.
    #   MRM en entrée   = matchés + à supprimer + non mappés (CPT_LATE exclu : il
    #                     provient d'un autre inventaire ou n'a pas de contrepartie MRM).
    #   COMPTE en entrée = matchés + récupérés tardifs + CPT_ONLY définitifs.
    mrm_pm_total = pm_match_mrm + pm_del + pm_miss
    cpt_pm_total = pm_match_cpt + pm_def + pm_late_cpt

    # ── Sous-ventilation de "Non mappés" : MISSING ∩ consigne ────────────────
    def miss(action):
        return mrm(lambda r: T(r) == "MRM_MISSING" and A(r) == action)
    keep_miss_nb,  keep_miss_pm  = miss("MRM_KEEP")
    study_miss_nb, study_miss_pm = miss("MRM_STUDY")
    add_miss_nb,   add_miss_pm   = miss("MRM_ADD")

    # ── Suivi des consignes (univers MRM principal : matchés légitimes + MISSING) ─
    # EXCLUT les tardifs (CPT_LATE : consigne issue d'un autre inventaire ou absente).
    # nb / conformité : tous les dossiers principaux de la consigne.
    # PM (MRM, Compte, Δ), taux de chute, volumétrie PM nulle/non-nulle : dossiers
    # MATCHÉS seulement. Pour "à supprimer", l'analyse PM n'est pas pertinente.
    def consigne(action):
        if action == "MRM_DELETE":
            univ = lambda r: A(r) == action and (T(r) in match or T(r) == "MRM_DELETE")
            conf = lambda r: univ(r) and T(r) not in match     # conforme = écarté
        else:
            univ = lambda r: A(r) == action and (T(r) in match or T(r) == "MRM_MISSING")
            conf = lambda r: univ(r) and T(r) in match          # conforme = retrouvé
        nb       = agg("nb", univ)
        conf_nb  = agg("nb", conf)
        is_m     = lambda r: univ(r) and T(r) in match
        nb_m     = agg("nb",           is_m)
        pm_mrm_m = agg("pm_mrm",       is_m)
        pm_cpt_m = agg("pm_cpt",       is_m)
        nz       = agg("nb_pm_mrm_nz", is_m)   # PM MRM ≠ 0
        nz0      = nb_m - nz                    # PM MRM nulle (null ou 0)
        delta    = pm_mrm_m - pm_cpt_m
        return {
            "nb": nb, "conf": conf_nb, "pct": _pct(conf_nb, nb),
            "nb_match": nb_m,
            "nz": nz,   "pct_nz":  _pct(nz, nb_m),
            "nz0": nz0, "pct_nz0": _pct(nz0, nb_m),
            "pm_mrm": pm_mrm_m, "pm_cpt": pm_cpt_m, "delta": delta,
            "taux_chute": _pct(delta, pm_mrm_m),
            "pertinent": action != "MRM_DELETE",
        }

    keep   = consigne("MRM_KEEP")
    study  = consigne("MRM_STUDY")
    add    = consigne("MRM_ADD")
    delete = consigne("MRM_DELETE")

    # ── Conformité globale (KEEP+ADD+STUDY, univers principal) ────────────────
    total_kas = agg("nb", lambda r: A(r) in _KAS and (T(r) in match or T(r) == "MRM_MISSING"))
    conf_kas  = agg("nb", lambda r: A(r) in _KAS and T(r) in match)

    # ── Taux de chute (KEEP/ADD/STUDY, univers métriques = matchés + tardifs) ─
    in_kas_metrics = lambda r: A(r) in _KAS and in_metrics(r)
    pm_mrm_kas = agg("pm_mrm", in_kas_metrics)
    pm_cpt_kas = agg("pm_cpt", in_kas_metrics)

    nb_trouves = nb_match + nb_late   # matchés inventaire + récupérés tardifs

    # ── Invariant de cohérence ────────────────────────────────────────────────
    # Toute ligne de df_result doit tomber dans exactement une catégorie connue.
    # Les lignes matchées sont physiquement uniques (CPT joint à MRM) → on les
    # compte une fois. classified < total_rows ⇒ un TYPE_RECONCILIATION inattendu
    # (label orphelin, étape oubliée) n'est pas pris en compte par la synthèse.
    total_rows      = sum(r["nb"] for r in rows)
    classified_rows = nb_match + nb_del + nb_miss + nb_def + nb_late
    labels_connus   = match | {"MRM_DELETE", "MRM_MISSING", "CPT_ONLY", "CPT_LATE"}
    labels_inconnus = sorted({T(r) for r in rows if T(r) not in labels_connus})

    return {
        # ── Bulle MRM ──
        "mrm_nb"          : nb_match + nb_del + nb_miss,
        "mrm_pm"          : mrm_pm_total,
        "a_supprimer_nb"  : nb_del,             "a_supprimer_pm"  : pm_del,
        "a_comparer_nb"   : nb_match + nb_miss, "a_comparer_pm"   : pm_match_mrm + pm_miss,
        "principale_nb"   : nb_princ,           "principale_pm"   : pm_princ,
        "affinee_nb"      : nb_aff,             "affinee_pm_mrm"  : pm_aff_mrm,
        "recup_nb"        : nb_recup,           "recup_pm_mrm"    : pm_recup_mrm,
        "non_mappes_nb"   : nb_miss,            "non_mappes_pm"   : pm_miss,
        "keep_nb"  : keep_miss_nb,   "keep_pm"  : keep_miss_pm,
        "study_nb" : study_miss_nb,  "study_pm" : study_miss_pm,
        "add_nb"   : add_miss_nb,    "add_pm"   : add_miss_pm,
        # ── Bulle MATCHÉS (légitimes inventaire) ──
        "match_nb"        : nb_match,
        "match_pm_mrm"    : pm_match_mrm,
        "match_pm_cpt"    : pm_match_cpt,
        "match_pm_ecart"  : pm_match_mrm - pm_match_cpt,
        # ── Bulle COMPTE ──
        "cpt_nb"          : nb_match + nb_def + nb_late,
        "cpt_pm"          : cpt_pm_total,
        "trouves_nb"      : nb_trouves,
        "late_nb" : nb_late, "late_pm" : pm_late_cpt,
        "late_pm_mrm" : pm_late_mrm, "late_pm_cpt" : pm_late_cpt,
        "late_n1_nb"  : nb_late_n1,
        "late_obs_nb" : nb_late_obs, "late_obs_pm" : pm_late_obs,
        "def_nb"  : nb_def,  "def_pm"  : pm_def,
        # ── Indicateurs (taux) ──
        "taux_couverture_mrm" : _pct(nb_match, nb_match + nb_miss),
        "taux_recup_tardive"  : _pct(nb_late, nb_late + nb_def),
        "taux_recup_globale"  : _pct(nb_trouves, nb_match + nb_late + nb_def),
        "taux_chute_global"   : _pct(pm_mrm_kas - pm_cpt_kas, pm_mrm_kas),
        "conformite_globale"  : _pct(conf_kas, total_kas),
        # ── Niveaux de PM (matchés légitimes + tardifs) ──
        "metrics_pm_mrm"   : pm_metrics_mrm,
        "metrics_pm_cpt"   : pm_metrics_cpt,
        "metrics_pm_ecart" : pm_metrics_mrm - pm_metrics_cpt,
        "metrics_pm_pct"   : _pct(pm_metrics_mrm - pm_metrics_cpt, pm_metrics_mrm),
        "metrics_nb"       : nb_metrics,
        # ── Suivi des consignes (détail) ──
        "consignes" : {
            "À conserver" : keep,
            "À étudier"   : study,
            "À ajouter"   : add,
            "À supprimer" : delete,
        },
        # ── Invariant de cohérence ──
        "total_rows"      : total_rows,
        "classified_rows" : classified_rows,
        "coherent"        : total_rows == classified_rows,
        "labels_inconnus" : labels_inconnus,
        # ── Entête ──
        "date_inventaire" : _resolve_date_inventaire(df_result),
    }


def _resolve_date_inventaire(df_result: DataFrame) -> str:
    """Date d'inventaire : figée (profile) ou max(MRM_D_INVENTAIRE) si 'auto'."""
    if DATE_INVENTAIRE != "auto":
        return DATE_INVENTAIRE
    if "MRM_D_INVENTAIRE" not in df_result.columns:
        return "n/d"
    row = df_result.agg(
        F.date_format(F.max("MRM_D_INVENTAIRE"), "dd/MM/yyyy").alias("d")
    ).first()
    return (row and row["d"]) or "n/d"


# ============================================================================
# RENDU ASCII
# ============================================================================

_B   = 34          # largeur d'une bulle
_LBL = 27          # largeur du libellé dans la colonne latérale
_RW  = 50          # largeur de la colonne latérale (libellé + nb + PM)
_T   = 3 + _B + 3 + _RW   # largeur du contenu intérieur de la boîte


def _n(x) -> str:
    """Entier avec séparateur de milliers espace (style FR)."""
    return f"{int(round(x or 0)):,}".replace(",", " ")


def _row(label: str, nb, pm) -> str:
    """Ligne latérale : 'libellé : nb  PM €' (nb et PM alignés à droite).

    pm=None → colonne PM laissée vide (sous-total sans contrepartie PM pertinente).
    """
    pm_txt = " " * 13 if pm is None else f"{_n(pm):>11} €"
    return f"{label:<{_LBL}}: {_n(nb):>6}  {pm_txt}"


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
            f"MATCHÉS = {_n(d['match_nb'])} ({_n(d['principale_nb'])}+{_n(d['affinee_nb'])}+{_n(d['recup_nb'])})",
            f" PM MRM   {_n(d['match_pm_mrm'])} €",
            f" PM CPT   {_n(d['match_pm_cpt'])} €",
            f" Δ PM     {_n(d['match_pm_ecart'])} €",
        )
        + _bubble(
            f"COMPTE = {_n(d['cpt_nb'])} dossiers",
            f"      / PM {_n(d['cpt_pm'])} €",
        )
    )

    right = [
        "",
        _row("Consigne à supprimer",  d["a_supprimer_nb"], d["a_supprimer_pm"]),
        _row("À comparer",            d["a_comparer_nb"],  d["a_comparer_pm"]),
        _row("Mappés clé principale", d["principale_nb"],  d["principale_pm"]),
        _row("Mappés clé affinée",    d["affinee_nb"],     d["affinee_pm_mrm"]),
        _row("Mappés récupération",   d["recup_nb"],       d["recup_pm_mrm"]),
        _row("Non mappés (MISSING)",  d["non_mappes_nb"],  d["non_mappes_pm"]),
        _row("├ à conserver",         d["keep_nb"],        d["keep_pm"]),
        _row("├ à étudier",           d["study_nb"],       d["study_pm"]),
        _row("└ à ajouter",           d["add_nb"],         d["add_pm"]),
        "",
        _row("Total CPT (compte)",          d["cpt_nb"],     d["cpt_pm"]),
        _row("├ matchés inventaire",        d["match_nb"],   d["match_pm_cpt"]),
        _row("├ récupérés tardifs",         d["late_nb"],    d["late_pm"]),
        _row("│  ├ retrouvés N+1",          d["late_n1_nb"], None),
        _row("│  └ observ. tardive IT",     d["late_obs_nb"], d["late_obs_pm"]),
        _row("└ CPT_ONLY définitifs",       d["def_nb"],     d["def_pm"]),
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
        "INDICATEURS",
        f"  Taux de couverture MRM (matchés / à comparer) : {d['taux_couverture_mrm']:>5} %",
        f"  Taux de récupération tardive (N+1)            : {d['taux_recup_tardive']:>5} %",
        f"  Taux de récupération globale (CPT trouvés)    : {d['taux_recup_globale']:>5} %",
        f"  Taux de chute global (KEEP/ADD/STUDY)         : {d['taux_chute_global']:>5} %",
        f"  Conformité globale des consignes              : {d['conformite_globale']:>5} %",
        "",
        f"NIVEAUX DE PM (matchés + tardifs, {_n(d['metrics_nb'])} dossiers)",
        f"  PM MRM   : {_n(d['metrics_pm_mrm']):>15} €",
        f"  PM CPT   : {_n(d['metrics_pm_cpt']):>15} €",
        f"  Écart    : {_n(d['metrics_pm_ecart']):>15} €  ({d['metrics_pm_pct']} %)",
        "",
        f"DÉCLARATIONS TARDIVES ({_n(d['late_nb'])} dossiers, incluses dans les métriques)",
        f"  ├ retrouvées N+1        : {_n(d['late_n1_nb'])} dossiers  "
        f"(PM MRM {_n(d['late_pm_mrm'])} € | PM CPT {_n(d['late_pm_cpt'])} €)",
        f"  └ observations tardives IT (garantie 60, fin d'année) : "
        f"{_n(d['late_obs_nb'])} dossiers  (PM CPT {_n(d['late_obs_pm'])} €)",
        "",
        f"CONTRÔLE DE COHÉRENCE : {coh} — {detail_coh}",
    ]
    return "\n".join(lines)


def _np(n, p) -> str:
    """Formate 'n (p%)' — volumétrie avec pourcentage entre parenthèses."""
    return f"{_n(n)} ({p}%)"


def _render_consignes(d: dict) -> str:
    """
    Suivi des consignes :
      - nb / %conf : sur tous les dossiers de la consigne
      - matchés, PM nulle/non-nulle (avec %), PM MRM/CPT, taux de chute :
        sur les dossiers matchés. "À supprimer" → analyse PM non pertinente.
    """
    head = (f"  {'Consigne':<13}{'nb':>6}{'%conf':>8}{'match.':>7}"
            f"{'PM MRM nulle':>13}{'PM MRM≠0':>14}{'PM MRM':>15}{'PM CPT':>15}{'chute':>8}")
    lines = [
        "SUIVI DES CONSIGNES — conformité (tous dossiers) ; PM & chute (dossiers matchés)",
        head,
    ]
    for label, c in d["consignes"].items():
        base = f"  {label:<13}{_n(c['nb']):>6}{c['pct']:>6} %{_n(c['nb_match']):>7}"
        if not c["pertinent"]:
            lines.append(base + "   — analyse PM non pertinente (consigne à supprimer) —")
        else:
            lines.append(
                base
                + f"{_np(c['nz0'], c['pct_nz0']):>13}{_np(c['nz'], c['pct_nz']):>14}"
                + f"{_n(c['pm_mrm']):>13} €{_n(c['pm_cpt']):>13} €{c['taux_chute']:>6} %"
            )
    return "\n".join(lines)


def render_synthese(d: dict, client: str = CLIENT_NAME) -> str:
    """Rend la synthèse complète : box + indicateurs + consignes."""
    return "\n\n".join([
        _render_box(d, client),
        _render_indicateurs(d),
        _render_consignes(d),
    ])


@timed_fn("print_synthese")
def print_synthese(df_result: DataFrame) -> dict:
    """Calcule + affiche la synthèse. Retourne les scalaires."""
    d = compute_synthese(df_result)
    print("\n" + render_synthese(d) + "\n")
    return d
