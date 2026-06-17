"""
Contrôles qualité des données brutes — défensifs et NON bloquants.

Pensés pour l'industrialisation : tout est tracé en WARNING (logger), le run
n'est JAMAIS interrompu (choix « WARN + continue »). Quand le pipeline sera
automatisé (Job Databricks), ces logs permettent de déboguer a posteriori sans
casser la production. Le DataFrame est toujours renvoyé inchangé.

PÉRIMÈTRE DU CONTRÔLE : uniquement les colonnes RÉELLEMENT UTILISÉES par le
pipeline — c.-à-d. les clés source des mappings (MAPPING_CPT / MAPPING_MRM,
cf. config/mappings.py). Les tables brutes CPT/MRM portent des dizaines de
colonnes non consommées (dont des colonnes « PM… » au nom perdu) : les contrôler
ne ferait que du bruit dans les logs et coûterait une agrégation inutile sur
tout le schéma. On se concentre donc sur ce que le pipeline lit vraiment :
    1. Colonnes attendues (du mapping) absentes de la source.
    2. Colonnes attendues présentes mais entièrement nulles (100 % de null).

`colonnes_nom_invalide` reste disponible comme diagnostic AD HOC du schéma brut
complet (noms nuls/vides/`_cN`), à appeler manuellement — il n'est pas branché au
pipeline pour ne pas polluer les logs avec des colonnes non utilisées.
"""

import logging
import re
from typing import Iterable, List

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

# Header CSV manquant : Spark nomme la colonne `_c<index>` (ex. "_c12").
_AUTO_NAME = re.compile(r"^_c\d+$")


def colonnes_nom_invalide(columns: Iterable[str]) -> List[str]:
    """Colonnes dont le NOM est nul, vide/blanc, ou auto-généré (`_c\\d+`).

    Diagnostic AD HOC du schéma brut complet (non branché au pipeline) : sert à
    inspecter manuellement une source suspecte (header manquant → colonnes « PM… »
    au nom perdu). Le contrôle du pipeline, lui, ne regarde que les colonnes du
    mapping (cf. controle_colonnes)."""
    invalides = []
    for c in columns:
        if c is None or str(c).strip() == "" or _AUTO_NAME.match(str(c)):
            invalides.append(c)
    return invalides


def controle_colonnes(
    df             : DataFrame,
    source_label   : str,
    used_cols      : Iterable[str],
    check_full_null: bool = True,
) -> DataFrame:
    """
    Contrôle qualité non bloquant, LIMITÉ aux colonnes utilisées par le pipeline.

    Args:
        df              : DataFrame brut à contrôler.
        source_label    : préfixe des logs pour la traçabilité ("CPT" / "MRM").
        used_cols       : colonnes réellement consommées = clés source du mapping
                          (MAPPING_CPT.keys() / MAPPING_MRM.keys()). SEULES ces
                          colonnes sont contrôlées (pas tout le schéma brut).
        check_full_null : si True, détecte les colonnes (utilisées) entièrement
                          nulles — coût = UNE action Spark, désormais limitée aux
                          colonnes du mapping (≈ 15-20, pas tout le schéma).

    Returns:
        Le DataFrame inchangé (le run continue toujours).
    """
    used    = list(used_cols)
    present = [c for c in used if c in df.columns]
    absent  = [c for c in used if c not in df.columns]
    logger.info(
        "[%s] contrôle colonnes utilisées (mapping) : %d/%d présentes.",
        source_label, len(present), len(used),
    )

    # 1. Colonnes du mapping absentes (complète le warning de select_and_rename).
    if absent:
        logger.warning(
            "[%s] %d colonne(s) du mapping absente(s) de la source : %s.",
            source_label, len(absent), absent,
        )

    # 2. Colonnes du mapping entièrement nulles — une seule passe d'agrégation,
    #    restreinte aux colonnes utilisées (plus de scan de tout le schéma brut).
    if check_full_null and present:
        # count(col) compte les NON-NULL → 0 = colonne entièrement nulle.
        agg = df.select([F.count(F.col(f"`{c}`")).alias(c) for c in present]).first()
        full_null = [c for c in present if (agg[c] or 0) == 0]
        if full_null:
            logger.warning(
                "[%s] %d colonne(s) utilisée(s) entièrement nulle(s) : %s.",
                source_label, len(full_null), full_null,
            )
        else:
            logger.info("[%s] aucune colonne utilisée entièrement nulle.", source_label)

    return df
