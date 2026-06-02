"""
Synthèse client — vue compacte type "schéma SODEXO".

Produit à chaque run une vue d'ensemble en 3 bulles (MRM / MATCHÉS / COMPTE)
avec, pour CHAQUE sous-catégorie, la volumétrie en **nombre de dossiers** et
en **PM (€)**. Rendue en ASCII dans la console.

Décodage des grandeurs (depuis df_result + TYPE_RECONCILIATION) :

    MRM      = MATCHÉS + à supprimer + non mappés (total dossiers MRM en entrée)
    MATCHÉS  = clé principale (EXACT + WINDOW) + clé affinée (TRONC + TRONC_WINDOW)
    COMPTE   = MATCHÉS + CPT non retrouvés (CPT_LATE + CPT_ONLY)

    CPT non retrouvés = récupérés clé affinée + déclarations tardives (N+1) + CPT_ONLY
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
)
from modules.matching import categorize_mrm_conclusion
from modules._timing import timed_fn


# ============================================================================
# CALCUL DES SCALAIRES (une seule passe Spark)
# ============================================================================

def compute_synthese(df_result: DataFrame) -> dict:
    """
    Agrège df_result en une passe (nb + PM MRM + PM CPT par catégorie) et
    retourne les scalaires de la synthèse.

    Args:
        df_result : DataFrame réconcilié (sortie de matching_waterfall, +
                    recover_late_declarations le cas échéant). Colonnes attendues :
                    TYPE_RECONCILIATION, MRM_PM, CPT_PM, MRM_CONCLUSION.

    Returns:
        dict des scalaires consommé par render_synthese().
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
        )
        .collect()
    )

    princ, aff, match = set(MATCH_PRINCIPALE), set(MATCH_AFFINEE), set(MATCH_LABELS)
    T = lambda r: r["TYPE_RECONCILIATION"]
    A = lambda r: r["MRM_ACTION"]

    def agg(field, pred):
        return sum(r[field] for r in rows if pred(r))

    def mrm(pred):   # → (nb, pm_mrm)
        return agg("nb", pred), agg("pm_mrm", pred)

    def cpt(pred):   # → (nb, pm_cpt)
        return agg("nb", pred), agg("pm_cpt", pred)

    matched = match | {"MATCH_IP"}

    nb_princ, pm_princ    = mrm(lambda r: T(r) in princ)
    nb_aff,   pm_aff_mrm  = mrm(lambda r: T(r) in aff)
    _,        pm_aff_cpt  = cpt(lambda r: T(r) in aff)
    nb_ip,    pm_ip_mrm   = mrm(lambda r: T(r) == "MATCH_IP")
    _,        pm_ip_cpt   = cpt(lambda r: T(r) == "MATCH_IP")
    nb_del,   pm_del      = mrm(lambda r: T(r) == "MRM_DELETE")
    nb_miss,  pm_miss     = mrm(lambda r: T(r) == "MRM_MISSING")
    nb_def,   pm_def      = cpt(lambda r: T(r) == "CPT_ONLY")
    nb_late,  pm_late     = cpt(lambda r: T(r) == "CPT_LATE")

    nb_match     = nb_princ + nb_aff + nb_ip
    pm_match_mrm = agg("pm_mrm", lambda r: T(r) in matched)
    pm_match_cpt = agg("pm_cpt", lambda r: T(r) in matched)

    def miss(action):
        return mrm(lambda r: T(r) == "MRM_MISSING" and A(r) == action)

    keep_nb,  keep_pm  = miss("MRM_KEEP")
    study_nb, study_pm = miss("MRM_STUDY")
    add_nb,   add_pm   = miss("MRM_ADD")

    return {
        # ── Bulle MRM ──
        "mrm_nb"          : nb_match + nb_del + nb_miss,
        "mrm_pm"          : agg("pm_mrm", lambda r: True),
        "a_supprimer_nb"  : nb_del,            "a_supprimer_pm"  : pm_del,
        "a_comparer_nb"   : nb_match + nb_miss, "a_comparer_pm"  : pm_match_mrm + pm_miss,
        "principale_nb"   : nb_princ,          "principale_pm"   : pm_princ,
        "affinee_nb"      : nb_aff,            "affinee_pm_mrm"  : pm_aff_mrm,
        "ip_nb"           : nb_ip,             "ip_pm_mrm"       : pm_ip_mrm,
        "non_mappes_nb"   : nb_miss,           "non_mappes_pm"   : pm_miss,
        "keep_nb"  : keep_nb,  "keep_pm"  : keep_pm,
        "study_nb" : study_nb, "study_pm" : study_pm,
        "add_nb"   : add_nb,   "add_pm"   : add_pm,
        # ── Bulle MATCHÉS ──
        "match_nb"     : nb_match,
        "match_pm_mrm" : pm_match_mrm,
        "match_pm_cpt" : pm_match_cpt,
        # ── Bulle COMPTE ──
        "cpt_nb"          : nb_match + nb_def + nb_late,
        "cpt_pm"          : agg("pm_cpt", lambda r: True),
        "non_retrouves_nb": nb_aff + nb_ip + nb_def + nb_late,
        "non_retrouves_pm": pm_aff_cpt + pm_ip_cpt + pm_def + pm_late,
        "affinee_cpt_pm"  : pm_aff_cpt,
        "ip_cpt_pm"       : pm_ip_cpt,
        "late_nb" : nb_late, "late_pm" : pm_late,
        "def_nb"  : nb_def,  "def_pm"  : pm_def,
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


def render_synthese(d: dict, client: str = CLIENT_NAME) -> str:
    """Rend le schéma SODEXO en ASCII à partir des scalaires."""
    left = (
        _bubble(
            f"MRM = {_n(d['mrm_nb'])} dossiers",
            f"     / PM {_n(d['mrm_pm'])} €",
        )
        + _bubble(
            f"MATCHÉS = {_n(d['match_nb'])} ({_n(d['principale_nb'])}+{_n(d['affinee_nb'])}+{_n(d['ip_nb'])})",
            f" PM MRM   {_n(d['match_pm_mrm'])} €",
            f" PM CPT   {_n(d['match_pm_cpt'])} €",
            f" Δ PM     {_n(d['match_pm_mrm'] - d['match_pm_cpt'])} €",
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
        _row("Mappés passage IP",     d["ip_nb"],          d["ip_pm_mrm"]),
        _row("Non mappés (MISSING)",  d["non_mappes_nb"],  d["non_mappes_pm"]),
        _row("├ à conserver",         d["keep_nb"],        d["keep_pm"]),
        _row("├ à étudier",           d["study_nb"],       d["study_pm"]),
        _row("└ à ajouter",           d["add_nb"],         d["add_pm"]),
        "",
        _row("Dossiers CPT non retrouvés", d["non_retrouves_nb"], d["non_retrouves_pm"]),
        _row("├ récupérés clé affinée",    d["affinee_nb"],       d["affinee_cpt_pm"]),
        _row("├ récupérés passage IP",     d["ip_nb"],            d["ip_cpt_pm"]),
        _row("├ déclarations tardives",    d["late_nb"],          d["late_pm"]),
        _row("└ CPT_ONLY définitifs",      d["def_nb"],           d["def_pm"]),
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


@timed_fn("print_synthese")
def print_synthese(df_result: DataFrame) -> dict:
    """Calcule + affiche la synthèse. Retourne les scalaires."""
    d = compute_synthese(df_result)
    print("\n" + render_synthese(d) + "\n")
    return d
