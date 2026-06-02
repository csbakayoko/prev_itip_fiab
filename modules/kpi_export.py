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
               + récupération (IP / rechute / date_retard)
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

    rows = (
        df.groupBy("TYPE_RECONCILIATION", "MRM_ACTION")
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

    princ = set(MATCH_PRINCIPALE)
    aff   = set(MATCH_AFFINEE)
    recup = set(MATCH_RECUPERATION)
    match = set(MATCH_LABELS)
    T = lambda r: r["TYPE_RECONCILIATION"]
    A = lambda r: r["MRM_ACTION"]

    def agg(field, pred):
        return sum(r[field] for r in rows if pred(r))

    def mrm(pred):   # → (nb, pm_mrm)
        return agg("nb", pred), agg("pm_mrm", pred)

    def cpt(pred):   # → (nb, pm_cpt)
        return agg("nb", pred), agg("pm_cpt", pred)

    nb_princ, pm_princ      = mrm(lambda r: T(r) in princ)
    nb_aff,   pm_aff_mrm    = mrm(lambda r: T(r) in aff)
    _,        pm_aff_cpt    = cpt(lambda r: T(r) in aff)
    nb_recup, pm_recup_mrm  = mrm(lambda r: T(r) in recup)
    _,        pm_recup_cpt  = cpt(lambda r: T(r) in recup)
    nb_del,   pm_del        = mrm(lambda r: T(r) == "MRM_DELETE")
    nb_miss,  pm_miss       = mrm(lambda r: T(r) == "MRM_MISSING")
    nb_def,   pm_def        = cpt(lambda r: T(r) == "CPT_ONLY")
    nb_late,  pm_late       = cpt(lambda r: T(r) == "CPT_LATE")

    nb_match     = nb_princ + nb_aff + nb_recup
    pm_match_mrm = agg("pm_mrm", lambda r: T(r) in match)
    pm_match_cpt = agg("pm_cpt", lambda r: T(r) in match)

    # ── Ventilation par consigne (nb, PM, et volumétrie PM≠0) ────────────────
    def consigne(action):
        nb  = agg("nb",          lambda r: A(r) == action)
        pm  = agg("pm_mrm",      lambda r: A(r) == action)
        nz  = agg("nb_pm_mrm_nz", lambda r: A(r) == action)
        # conforme = matché pour KEEP/ADD/STUDY ; non matché pour DELETE
        if action == "MRM_DELETE":
            conf = agg("nb", lambda r: A(r) == action and T(r) not in match)
        else:
            conf = agg("nb", lambda r: A(r) == action and T(r) in match)
        return {"nb": nb, "pm": pm, "nz": nz, "conf": conf, "pct": _pct(conf, nb)}

    keep   = consigne("MRM_KEEP")
    study  = consigne("MRM_STUDY")
    add    = consigne("MRM_ADD")
    delete = consigne("MRM_DELETE")

    # ── Conformité globale (KEEP + ADD + STUDY) ──────────────────────────────
    total_kas = agg("nb", lambda r: A(r) in _KAS)
    conf_kas  = agg("nb", lambda r: A(r) in _KAS and T(r) in match)

    # ── Taux de chute global (matchés KEEP/ADD/STUDY) ────────────────────────
    pm_mrm_kas = agg("pm_mrm", lambda r: A(r) in _KAS and T(r) in match)
    pm_cpt_kas = agg("pm_cpt", lambda r: A(r) in _KAS and T(r) in match)

    nb_trouves = nb_match + nb_late   # tout ce qui est rattaché à un MRM

    return {
        # ── Bulle MRM ──
        "mrm_nb"          : nb_match + nb_del + nb_miss,
        "mrm_pm"          : agg("pm_mrm", lambda r: True),
        "a_supprimer_nb"  : nb_del,             "a_supprimer_pm"  : pm_del,
        "a_comparer_nb"   : nb_match + nb_miss, "a_comparer_pm"   : pm_match_mrm + pm_miss,
        "principale_nb"   : nb_princ,           "principale_pm"   : pm_princ,
        "affinee_nb"      : nb_aff,             "affinee_pm_mrm"  : pm_aff_mrm,
        "recup_nb"        : nb_recup,           "recup_pm_mrm"    : pm_recup_mrm,
        "non_mappes_nb"   : nb_miss,            "non_mappes_pm"   : pm_miss,
        "keep_nb"  : keep["nb"],   "keep_pm"  : keep["pm"],
        "study_nb" : study["nb"],  "study_pm" : study["pm"],
        "add_nb"   : add["nb"],    "add_pm"   : add["pm"],
        # ── Bulle MATCHÉS ──
        "match_nb"        : nb_match,
        "match_pm_mrm"    : pm_match_mrm,
        "match_pm_cpt"    : pm_match_cpt,
        "match_pm_ecart"  : pm_match_mrm - pm_match_cpt,
        # ── Bulle COMPTE ──
        "cpt_nb"          : nb_match + nb_def + nb_late,
        "cpt_pm"          : agg("pm_cpt", lambda r: True),
        "trouves_nb"      : nb_trouves,
        "non_retrouves_nb": nb_aff + nb_recup + nb_def + nb_late,
        "non_retrouves_pm": pm_aff_cpt + pm_recup_cpt + pm_def + pm_late,
        "affinee_cpt_pm"  : pm_aff_cpt,
        "recup_cpt_pm"    : pm_recup_cpt,
        "late_nb" : nb_late, "late_pm" : pm_late,
        "def_nb"  : nb_def,  "def_pm"  : pm_def,
        # ── Indicateurs (taux) ──
        "taux_couverture_mrm" : _pct(nb_match, nb_match + nb_miss),
        "taux_recup_tardive"  : _pct(nb_late, nb_late + nb_def),
        "taux_recup_globale"  : _pct(nb_trouves, nb_match + nb_late + nb_def),
        "taux_chute_global"   : _pct(pm_mrm_kas - pm_cpt_kas, pm_mrm_kas),
        "conformite_globale"  : _pct(conf_kas, total_kas),
        "pm_match_ecart_pct"  : _pct(pm_match_mrm - pm_match_cpt, pm_match_mrm),
        # ── Suivi des consignes (détail) ──
        "consignes" : {
            "À conserver" : keep,
            "À étudier"   : study,
            "À ajouter"   : add,
            "À supprimer" : delete,
        },
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
    """Ligne latérale : 'libellé : nb  PM €' (nb et PM alignés à droite)."""
    return f"{label:<{_LBL}}: {_n(nb):>6}  {_n(pm):>11} €"


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
        _row("CPT trouvés (match+tardifs)", d["trouves_nb"], d["match_pm_cpt"] + d["late_pm"]),
        _row("├ matchés inventaire",        d["match_nb"],   d["match_pm_cpt"]),
        _row("├ récupérés tardifs (N+1)",   d["late_nb"],    d["late_pm"]),
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
    """Bloc des taux globaux + niveaux de PM."""
    lines = [
        "INDICATEURS",
        f"  Taux de couverture MRM (matchés / à comparer) : {d['taux_couverture_mrm']:>5} %",
        f"  Taux de récupération tardive (N+1)            : {d['taux_recup_tardive']:>5} %",
        f"  Taux de récupération globale (CPT trouvés)    : {d['taux_recup_globale']:>5} %",
        f"  Taux de chute global (KEEP/ADD/STUDY)         : {d['taux_chute_global']:>5} %",
        f"  Conformité globale des consignes              : {d['conformite_globale']:>5} %",
        "",
        "NIVEAUX DE PM (matchés)",
        f"  PM MRM   : {_n(d['match_pm_mrm']):>15} €",
        f"  PM CPT   : {_n(d['match_pm_cpt']):>15} €",
        f"  Écart    : {_n(d['match_pm_ecart']):>15} €  ({d['pm_match_ecart_pct']} %)",
    ]
    return "\n".join(lines)


def _render_consignes(d: dict) -> str:
    """Bloc suivi des consignes : conformité + volumétrie PM≠0 par consigne."""
    head = f"  {'Consigne':<14}{'nb':>7}{'conf.':>8}{'%conf':>8}{'PM≠0':>8}   PM (€)"
    lines = ["SUIVI DES CONSIGNES (conformité + volumétrie PM non nulle)", head]
    for label, c in d["consignes"].items():
        lines.append(
            f"  {label:<14}{_n(c['nb']):>7}{_n(c['conf']):>8}{c['pct']:>7} %{_n(c['nz']):>8}   {_n(c['pm'])} €"
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
