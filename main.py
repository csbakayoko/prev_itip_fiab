"""
Pipeline de fiabilisation ITIP-FIAB — point d'entrée.

Spine essentiel, multi-périmètre :
    load → clean → waterfall → synthèse (ASCII console) → métriques + graphiques

Le calcul des indicateurs vit dans core.metrics (des fonctions qui
reshapent le dict de la synthèse — une passe Spark — en tables pandas),
leur mise en forme dans core.viz (9 graphiques-messages).

Périmètre piloté par config/profile.py (par défaut : toutes les clauses). Lancement :
    spark-submit main.py        (ou exécution dans un notebook Databricks)
"""


from pyspark.sql import DataFrame, SparkSession

from config import (
    db_cfg, tech_cfg, RUN_PARAMS,
    EXPORT_ANALYSES, EXPORT_FORMATS, EXPORT_DELTA_SCHEMA, EXPORT_GRAPHS,
    RECUP_NON_LABEL, CHECKPOINT_DIR,
)
from core.load_data import load_cpt_raw, load_mrm_raw
from core.transform import clean_cpt, clean_mrm
from core.matching import (
    matching_waterfall,
    recover_late_declarations,
    flag_late_it_observations,
    enrich_result_tags,
)
from core.metrics import export_metriques
from core.viz import restituer_graphiques
from core.kpi_export import print_synthese
from core._timing import timed
import pyspark.sql.functions as F


def _split_mrm_statut(mrm_clean: DataFrame, statut_col: str = "MRM_STATUT_INV"):
    """Scinde le MRM clean en (OUI + statut absent, NON).

    OUI alimente le matching principal ; NON est réservé à la passe de repêchage
    des CPT_ONLY (PM MRM = 0). null traité comme non-NON (→ OUI)."""
    if statut_col not in mrm_clean.columns:
        return mrm_clean, mrm_clean.limit(0)
    is_non = F.coalesce(F.upper(F.trim(F.col(statut_col))) == F.lit("NON"), F.lit(False))
    return mrm_clean.filter(~is_non), mrm_clean.filter(is_non)


def run(spark: SparkSession) -> DataFrame:
    """Exécute le pipeline complet et affiche la synthèse client."""
    # Checkpoints fiables (DBFS) : les matérialisations du waterfall survivent
    # à la perte d'un executor (autoscaling). Sans checkpointDir, _materialize
    # retombe sur localCheckpoint → CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND possible
    # dès que le cluster réduit en cours de run.
    if CHECKPOINT_DIR:
        spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

    with timed("PIPELINE TOTAL"):
        cpt_clean = clean_cpt(load_cpt_raw(spark, db_cfg), tech_cfg)
        mrm_clean = clean_mrm(load_mrm_raw(spark, db_cfg), tech_cfg)

        # Le statut inventaire NON ne sert PAS au matching principal : il est
        # réservé au repêchage des CPT_ONLY (PM MRM = 0, non remonté à la
        # direction financière). On scinde : OUI (+ statut absent) pour le
        # matching, NON pour la passe de repêchage dédiée.
        mrm_oui, mrm_non = _split_mrm_statut(mrm_clean)

        df_result = matching_waterfall(cpt_clean, mrm_oui)

        # Déclarations tardives : CPT_ONLY retrouvés dans l'inventaire MRM N+1.
        # Les dossiers récupérés sont enrichis des infos MRM (TYPE_RECONCILIATION=
        # CPT_LATE). Cascade RECOVERY_KEYS : le waterfall principal rejoué dans
        # l'ordre (strict → flexible), mêmes règles et mêmes fenêtres.
        # L'étape gagnante est tracée dans LATE_KEY.
        mrm_n1_non = None
        if RUN_PARAMS.get("fichier_mrm_n1"):
            mrm_n1 = clean_mrm(load_mrm_raw(spark, db_cfg, "fichier_mrm_n1"), tech_cfg)
            # Règle CPT_LATE (inchangée) : seuls les OUI (+ statut absent) du N+1
            # produisent des CPT_LATE — un NON du N+1 a une PM MRM = 0, le prendre
            # comme contrepartie fausserait l'univers de chute (CPT_LATE est
            # INCLUS dans les métriques). On scinde le N+1 : les OUI alimentent le
            # repêchage CPT_LATE ci-dessous, les NON rejoignent la passe statut
            # NON (repêchage hors métriques, comme le NON de l'exercice N).
            mrm_n1_oui, mrm_n1_non = _split_mrm_statut(mrm_n1)
            df_result = recover_late_declarations(df_result, [("MRM_N1", mrm_n1_oui)])

        # Repêchage via statut NON : les CPT_ONLY restants retrouvés dans les MRM
        # NON sont tagués CPT_RECUP_NON. Label distinct → EXCLU de toutes les
        # métriques, présenté dans une analyse dédiée. Le statut NON est repêché
        # sur les DEUX exercices, avec un LATE_SOURCE distinct (STATUT_NON pour N,
        # STATUT_NON_N1 pour N+1) → la part de chaque exercice est ventilée dans
        # la restitution. Les MRM NON non repêchés ne sont jamais unionnés (ils
        # disparaissent, zéro empreinte). Même cascade RECOVERY_KEYS que le N+1.
        non_inventories = [("STATUT_NON", mrm_non)]
        if mrm_n1_non is not None:
            non_inventories.append(("STATUT_NON_N1", mrm_n1_non))
        df_result = recover_late_declarations(
            df_result, non_inventories, label=RECUP_NON_LABEL,
        )

        # Observations tardives IT : CPT_ONLY garantie 60 survenus en fin d'année,
        # absents du MRM courant et du N+1 → tagués CPT_OBS_TARDIVE (anomalie,
        # LATE_SOURCE=OBS_TARDIVE_IT). Jamais matchés → exclus des taux et des PM.
        df_result = flag_late_it_observations(df_result)

        # Colonnes persistantes : consigne reformatée (MRM_ACTION) + tag des
        # orphelins CPT_ONLY (TAG_CPT_ONLY).
        df_result = enrich_result_tags(df_result)

        with timed("persist df_result"):
            df_result = df_result.persist()
            df_result.count()  # force la matérialisation pour un timing fiable

        # ====================================================================
        # RESTITUTION : synthèse console, puis métriques + graphiques.
        # Univers PM/chute = matchés + récupérés N+1 ; obs tardives IT,
        # MRM_MISSING et CPT_ONLY exclus des comparaisons.
        # ====================================================================

        # ÉTAPE 0 — vue d'ensemble + taux distincts (matching vs récupération).
        # print_synthese renvoie le dict de compute_synthese : la passe Spark
        # est faite UNE fois, réutilisée par les métriques et les graphiques.
        with timed("ÉTAPE 0 synthèse"):
            d = print_synthese(df_result)

        # ÉTAPE 1 — export des métriques (tables pandas sérialisées
        # CSV/JSON/Parquet/Excel/Delta sur DBFS).
        if EXPORT_ANALYSES:
            with timed("ÉTAPE 1 export métriques"):
                export_metriques(df_result, d, formats=EXPORT_FORMATS,
                                 delta_schema=EXPORT_DELTA_SCHEMA)

        # ÉTAPE 2 — graphiques de restitution (titres-messages : justification
        # du compte, couverture des listes d'arrêts, chute par clause/consigne,
        # conformité des consignes, anomalies). Affichés en notebook + PNG DBFS.
        if EXPORT_GRAPHS:
            with timed("ÉTAPE 2 graphiques"):
                restituer_graphiques(df_result, d)
    return df_result


if __name__ == "__main__":
    spark = SparkSession.builder.appName("itip_fiab").getOrCreate()
    # AQE + skew join : critique pour les theta-joins des étapes windowed
    # (key + |datediff| <= N) qui sinon partent en shuffle déséquilibré.
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    run(spark)
