"""
Récupérations post-waterfall et tags persistants du résultat.

- recover_late_declarations : seconde chance des CPT_ONLY sur des inventaires
  ultérieurs (CPT_LATE) ou les MRM statut NON (CPT_RECUP_NON, hors métriques) ;
- flag_late_it_observations : obs tardives IT (CPT_OBS_TARDIVE, hors métriques) ;
- enrich_result_tags : MRM_ACTION + segmentation TAG_CPT_ONLY.

⚠ DATE_INVENTAIRE est lue ICI (année d'inventaire des tags) : pour rejouer un
autre inventaire dans la même session, configurer_run réassigne
core.match.recovery.DATE_INVENTAIRE (cf. core/runtime.py).
"""

from functools import reduce
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from config import (
    DATE_INVENTAIRE, LATE_IT_GARANTIE, OBS_TARDIVE_LABEL,
    ORPHAN_FIN_ANNEE_MOIS, ORPHAN_PM_THRESHOLD, LOG_VOLUMETRIE,
)
from core._timing import timed_fn
from core.match.steps import RECOVERY_KEYS, _materialize
from core.match.waterfall import categorize_mrm_conclusion


# ============================================================================
# DÉCLARATIONS TARDIVES (MRM N+1, N+2, …)
# ============================================================================

@timed_fn("recover_late_declarations")
def recover_late_declarations(
    df_result  : DataFrame,
    inventories: List[Tuple[str, DataFrame]],
    keys       : Tuple = RECOVERY_KEYS,
    label      : str = "CPT_LATE",
) -> DataFrame:
    """
    Donne une seconde chance aux CPT_ONLY sur des inventaires MRM ultérieurs OU
    sur les MRM au statut inventaire NON (repêchage).

    Cascade : chaque CPT_ONLY est testé contre les inventaires dans l'ordre,
    et pour chaque inventaire contre les étapes `keys` dans l'ordre.
    RECOVERY_KEYS par défaut : le waterfall principal rejoué du plus strict au
    plus flexible (EXACT → WINDOW → TRONC → TRONC_WINDOW → IP → RECHUTE →
    RECHUTE_TRONC), cf. commentaire de la constante — l'ordre garantit qu'un
    assuré à plusieurs sinistres est rapproché de la bonne contrepartie.
    Une étape est (label, clé, condition supplémentaire ou None), ou un simple
    nom de clé. La première (inventaire, étape) qui contient le dossier le
    récupère :
        - TYPE_RECONCILIATION → `label` ("CPT_LATE" pour le N+1, "CPT_RECUP_NON"
                                pour le repêchage via statut NON)
        - LATE_SOURCE         → tag de l'inventaire (ex: "MRM_N1", "STATUT_NON")
        - LATE_KEY            → étape ayant permis le repêchage (traçabilité,
                                ex: "MATCH_EXACT", "MATCH_RECHUTE")
        - colonnes MRM_*      → enrichies depuis l'inventaire

    Le label conditionne l'inclusion dans les métriques : "CPT_LATE" est inclus
    (vraie contrepartie N+1) ; "CPT_RECUP_NON" est un label distinct → exclu par
    construction de toutes les métriques (cf. RECUP_NON_LABEL). Les MRM de
    l'inventaire qui ne matchent rien ne sont JAMAIS unionnés (un NON non
    repêché disparaît donc naturellement, sans empreinte volumétrique).

    Performance : chaque inventaire est matérialisé une seule fois (clés +
    colonnes MRM_*) avant la cascade, puis dédoublonné sur la clé courante et
    broadcasté à chaque étape ; le remaining est re-matérialisé après chaque
    étape fructueuse (lignée plate, pas de chaîne d'anti-joins recalculée).
    """
    steps = [(k, k, None) if isinstance(k, str) else k for k in keys]
    print(f"[late] === recovery démarré (label={label}, étapes={[s for s, _, _ in steps]}) ===")
    is_cpt_only = F.col("TYPE_RECONCILIATION") == "CPT_ONLY"
    rest = df_result.filter(~is_cpt_only)

    remaining_cpt = (
        df_result.filter(is_cpt_only)
                 .select(*[c for c in df_result.columns if not c.startswith("MRM_")])
    ).cache()
    # Informatif (la cache se matérialise au 1er join de la cascade) → gaté.
    if LOG_VOLUMETRIE:
        print(f"[late] CPT_ONLY initiaux : {remaining_cpt.count():,}")

    recovered: List[DataFrame] = []
    for tag, df_mrm in inventories:
        # Matérialise l'inventaire UNE SEULE FOIS (clés + colonnes MRM_*).
        # Sans ça, chaque étape de la cascade re-déroule tout le pipeline de
        # nettoyage de l'inventaire (lecture CSV + dédoublonnages fenêtrés)
        # pour construire son broadcast → coût multiplié par le nombre
        # d'étapes (cause directe des runs > 30 min).
        inv_cols = list(dict.fromkeys(
            [k for _, k, _ in steps if k in df_mrm.columns]
            + [c for c in df_mrm.columns if c.startswith("MRM_")]
        ))
        df_inv = _materialize(df_mrm.select(*inv_cols))

        for step_label, key, cond in steps:
            print(f"[late] ▶ {tag} ({step_label}, clé={key})")
            mrm_enrich = (
                df_inv.filter(F.col(key).isNotNull())
                      .select(key, *[c for c in df_inv.columns if c.startswith("MRM_")])
                      .dropDuplicates([key])
            )
            hit = remaining_cpt.join(F.broadcast(mrm_enrich), on=key, how="inner")
            if cond is not None:
                hit = hit.filter(cond())
            hit = (
                hit.withColumn("TYPE_RECONCILIATION", F.lit(label))
                   .withColumn("LATE_SOURCE", F.lit(tag))
                   .withColumn("LATE_KEY", F.lit(step_label))
            ).cache()
            n_hit = hit.count()
            print(f"[late]   ↳ {tag}/{step_label} : {n_hit:,} retrouvés")

            if n_hit:
                # Tronque la lignée du remaining (sinon chaîne d'anti-joins de
                # plus en plus profonde, recalculée par chaque étape suivante).
                # Étape sans hit → remaining inchangé, rien à faire.
                remaining_cpt = _materialize(
                    remaining_cpt.join(
                        F.broadcast(hit.select(key).distinct()), on=key, how="left_anti"
                    )
                )
                recovered.append(hit)

    df_final = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [rest, remaining_cpt, *recovered],
    ).cache()
    if LOG_VOLUMETRIE:
        print(f"[late]   ↳ union : {df_final.count():,} lignes")
    print("[late] === recovery terminé ===")
    return df_final


# ============================================================================
# ENRICHISSEMENT POST-MATCHING (tags persistants du résultat)
# ============================================================================

def _inventory_year() -> Optional[int]:
    """Année d'inventaire dérivée de DATE_INVENTAIRE ('dd/MM/yyyy'). None si 'auto'."""
    try:
        return int(str(DATE_INVENTAIRE).split("/")[-1])
    except (ValueError, AttributeError):
        return None


def flag_late_it_observations(
    df_result   : DataFrame,
    garantie_col: str = "CPT_GARANTIE",
    date_col    : str = "CPT_D_SURVENANCE",
) -> DataFrame:
    """
    Tague en CPT_OBS_TARDIVE les CPT_ONLY qui sont des observations tardives d'IT.

    Un CPT_ONLY resté orphelin après la récupération N+1 (donc absent du MRM
    courant ET du N+1 → « pas dans deux exercices successifs ») est calé en
    observation tardive lorsque :
        - garantie == LATE_IT_GARANTIE (incapacité de travail),
        - survenance en fin d'année (mois ∈ ORPHAN_FIN_ANNEE_MOIS),
        - année de survenance == année d'inventaire (si dérivable de DATE_INVENTAIRE).

    Hypothèse métier : la couverture IT a vraisemblablement pris fin avant la date
    d'inventaire de l'exercice suivant (inventaire MRM réalisé 2× par an), d'où
    l'absence de contrepartie MRM. Ces lignes n'ont donc pas de colonnes MRM_*.

    IMPORTANT — ce ne sont PAS des dossiers retrouvés : ils n'ont jamais matché.
    On les tague comme anomalie (déclaration probable de fin d'année) mais on les
    EXCLUT des taux (matching / récupération) et des calculs PM / taux de chute.
    Le label distinct OBS_TARDIVE_LABEL (≠ CPT_LATE) garantit cette exclusion par
    construction : tout code basé sur MATCH_LABELS / CPT_LATE les ignore.

    Lignes taguées :
        TYPE_RECONCILIATION → OBS_TARDIVE_LABEL ("CPT_OBS_TARDIVE")
        LATE_SOURCE         → "OBS_TARDIVE_IT"  (traçabilité de l'origine)

    À appeler APRÈS recover_late_declarations et AVANT enrich_result_tags (pour
    que ces dossiers ne soient plus comptés/tagués comme CPT_ONLY).
    """
    inv_year    = _inventory_year()
    is_cpt_only = F.col("TYPE_RECONCILIATION") == "CPT_ONLY"
    eligible = (
        is_cpt_only
        & (F.col(garantie_col).cast("int") == F.lit(LATE_IT_GARANTIE))
        & F.month(F.col(date_col)).isin(*ORPHAN_FIN_ANNEE_MOIS)
        & (F.year(F.col(date_col)) == F.lit(inv_year) if inv_year is not None else F.lit(True))
    )

    df = df_result
    if "LATE_SOURCE" not in df.columns:
        df = df.withColumn("LATE_SOURCE", F.lit(None).cast("string"))

    return (
        df.withColumn(
            "TYPE_RECONCILIATION",
            F.when(eligible, F.lit(OBS_TARDIVE_LABEL)).otherwise(F.col("TYPE_RECONCILIATION")),
        )
        .withColumn(
            "LATE_SOURCE",
            F.when(eligible, F.lit("OBS_TARDIVE_IT")).otherwise(F.col("LATE_SOURCE")),
        )
    )


def enrich_result_tags(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    date_col      : str = "CPT_D_SURVENANCE",
    pm_col        : str = "CPT_PM",
) -> DataFrame:
    """
    Ajoute deux colonnes persistantes au résultat de réconciliation :

    1. MRM_ACTION  : consigne MRM reformatée (MRM_KEEP / MRM_ADD / MRM_STUDY /
                     MRM_DELETE), null si pas de conclusion. Conservée pour le
                     reporting et Power BI (évite de recalculer ailleurs).

    2. TAG_CPT_ONLY : segmentation actionnable des CPT_ONLY définitifs —
                      DECLA_TARDIVE_PROBABLE  : survenance en fin d'année
                          d'inventaire (mois ∈ ORPHAN_FIN_ANNEE_MOIS) → sinistre
                          probablement déclaré après la clôture MRM.
                      ORPHELIN_MONTANT_ELEVE  : PM CPT > ORPHAN_PM_THRESHOLD.
                      ORPHELIN_A_ANALYSER     : les autres orphelins.
                      null pour les lignes non CPT_ONLY.

    À appeler après flag_late_it_observations (les obs tardives ne doivent plus
    être taguées comme CPT_ONLY).
    """
    df = df_result.withColumn("MRM_ACTION", categorize_mrm_conclusion(F.col(conclusion_col)))

    is_cpt_only = F.col("TYPE_RECONCILIATION") == "CPT_ONLY"
    inv_year    = _inventory_year()
    fin_annee   = (
        is_cpt_only
        & F.month(F.col(date_col)).isin(*ORPHAN_FIN_ANNEE_MOIS)
        & (F.year(F.col(date_col)) == F.lit(inv_year) if inv_year is not None else F.lit(True))
    )

    return df.withColumn(
        "TAG_CPT_ONLY",
        F.when(fin_annee,                                       "DECLA_TARDIVE_PROBABLE")
         .when(is_cpt_only & (F.col(pm_col) > ORPHAN_PM_THRESHOLD), "ORPHELIN_MONTANT_ELEVE")
         .when(is_cpt_only,                                     "ORPHELIN_A_ANALYSER")
         .otherwise(None)
    )
