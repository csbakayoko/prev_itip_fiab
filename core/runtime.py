"""
Runtime du pipeline — session Spark partagée + surcharge de configuration.

`get_spark()` : LA session Spark du pipeline (réglages AQE appliqués), utilisée
par main.py et tous les notebooks — un seul endroit où régler la conf Spark.

`configurer_run()` : rejouer plusieurs inventaires dans une même session.
POURQUOI : les valeurs de `config/profile.py` sont liées **par valeur** dans les
modules consommateurs au moment de l'import (`from config import DATE_INVENTAIRE`).
Réassigner `config.DATE_INVENTAIRE` après coup n'a donc aucun effet. Pour rejouer
le pipeline sur plusieurs inventaires DANS UNE MÊME SESSION (ex. comparaison
2023 vs 2024), on réassigne les globals **là où ils sont effectivement lus** :

    - DATE_INVENTAIRE    → core.match.recovery  (tag obs tardives IT, année)
                           core.synthese.kpi_export (date de la synthèse)
    - CLIENT_CPT_VISION  → core.io.load_data    (filtre vision du compte)
    - fichiers MRM       → config.RUN_PARAMS    (dict PARTAGÉ : sa mutation est
                           visible partout sans réassignation)

À appeler AVANT `build_df_result` ET avant `compute_synthese`. La date pilote
aussi le découpage par ancienneté (N / N-1 / N-2+, via `d["date_inventaire"]`).

Limites : ne touche PAS au filtrage clause/type (identique entre inventaires
d'un même périmètre) ni au nommage des exports (édit `profile.py` pour un export
historisé par run). C'est un utilitaire de notebook/analyse, pas un chemin prod.
"""

from typing import Optional

from pyspark.sql import SparkSession

from config import RUN_PARAMS


def get_spark(app_name: str = "itip_fiab") -> SparkSession:
    """Session Spark du pipeline, réglages appliqués.

    AQE + skew join : critique pour les theta-joins des étapes windowed
    (key + |datediff| <= N) qui sinon partent en shuffle déséquilibré.

    Args:
        app_name : nom d'application Spark (défaut "itip_fiab").

    Returns:
        SparkSession active (créée ou réutilisée), conf AQE posée.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    return spark


def configurer_run(
    *,
    date_inventaire: str,
    cpt_vision     : str,
    fichier_mrm    : str,
    fichier_mrm_n1 : Optional[str] = None,
) -> dict:
    """Applique la config d'UN inventaire aux modules déjà importés.

    Args:
        date_inventaire : "dd/MM/yyyy" (ou "auto") — date de l'inventaire courant.
        cpt_vision      : vision comptable CPT (ex. "CC2023", "CC2024").
        fichier_mrm     : chemin du MRM de l'inventaire courant.
        fichier_mrm_n1  : chemin du MRM N+1 (None = pas de récupération tardive).

    Returns:
        dict récapitulatif du run appliqué (traçabilité notebook).
    """
    import core.io.load_data as load_data
    import core.match.recovery as recovery
    import core.synthese.kpi_export as kpi_export

    load_data.CLIENT_CPT_VISION = cpt_vision
    recovery.DATE_INVENTAIRE    = date_inventaire
    kpi_export.DATE_INVENTAIRE  = date_inventaire

    # RUN_PARAMS est le dict PARTAGÉ lu par load_mrm_raw → mutation in place.
    RUN_PARAMS["fichier_mrm"] = fichier_mrm
    if fichier_mrm_n1:
        RUN_PARAMS["fichier_mrm_n1"] = fichier_mrm_n1
    else:
        RUN_PARAMS.pop("fichier_mrm_n1", None)

    return {
        "date_inventaire": date_inventaire,
        "cpt_vision"     : cpt_vision,
        "fichier_mrm"    : fichier_mrm,
        "fichier_mrm_n1" : fichier_mrm_n1,
    }
