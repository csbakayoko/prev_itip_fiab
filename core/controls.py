"""
Contrôles qualité des données brutes — défensifs et NON bloquants.

Pensés pour l'industrialisation : tout est tracé en WARNING (logger), le run
n'est JAMAIS interrompu (choix « WARN + continue »). Quand le pipeline sera
automatisé (Job Databricks), ces logs permettent de déboguer a posteriori sans
casser la production. Le DataFrame est toujours renvoyé inchangé.

Anomalies détectées (sur les colonnes brutes, avant select_and_rename) :
    1. Noms de colonnes invalides : nul / vide / blanc, ou auto-généré par Spark
       sur un header CSV manquant (`_c0`, `_c1`…). Symptôme typique des colonnes
       « PM… » au nom perdu à la source.
    2. Colonnes attendues absentes (clés du mapping non présentes).
    3. Colonnes entièrement nulles (taux de null = 100 %).
"""

import logging
import re
from typing import Iterable, List, Optional

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

# Header CSV manquant : Spark nomme la colonne `_c<index>` (ex. "_c12").
_AUTO_NAME = re.compile(r"^_c\d+$")


def colonnes_nom_invalide(columns: Iterable[str]) -> List[str]:
    """Colonnes dont le NOM est nul, vide/blanc, ou auto-généré (`_c\\d+`)."""
    invalides = []
    for c in columns:
        if c is None or str(c).strip() == "" or _AUTO_NAME.match(str(c)):
            invalides.append(c)
    return invalides


def controle_colonnes(
    df             : DataFrame,
    source_label   : str,
    expected       : Optional[Iterable[str]] = None,
    check_full_null: bool = True,
) -> DataFrame:
    """
    Contrôle qualité non bloquant des colonnes d'un DataFrame brut.

    Args:
        df              : DataFrame brut à contrôler (colonnes source).
        source_label    : préfixe des logs pour la traçabilité ("CPT" / "MRM").
        expected        : noms de colonnes attendus (ex. MAPPING_*.keys()) ; les
                          absents sont loggés. None = pas de contrôle d'attendu.
        check_full_null : si True, détecte les colonnes entièrement nulles —
                          coût = UNE action Spark (agrégation), désactivable.

    Returns:
        Le DataFrame inchangé (le run continue toujours).
    """
    cols = list(df.columns)
    logger.info("[%s] contrôle colonnes : %d colonne(s) au total.", source_label, len(cols))

    # 1. Noms invalides (nul / vide / blanc / auto-généré `_cN`).
    invalides = colonnes_nom_invalide(cols)
    if invalides:
        positions = [cols.index(c) for c in invalides]
        logger.warning(
            "[%s] %d colonne(s) au nom invalide (nul/vide/`_cN`) : %s — positions %s. "
            "Header source probablement manquant (cf. colonnes « PM… »).",
            source_label, len(invalides), invalides, positions,
        )

    # 2. Colonnes attendues absentes (complète le warning de select_and_rename).
    if expected is not None:
        manquantes = [c for c in expected if c not in cols]
        if manquantes:
            logger.warning(
                "[%s] %d colonne(s) attendue(s) absente(s) : %s.",
                source_label, len(manquantes), manquantes,
            )

    # 3. Colonnes entièrement nulles — une seule passe d'agrégation.
    if check_full_null:
        valides = [c for c in cols if c and str(c).strip()]
        if valides:
            # count(col) compte les NON-NULL → 0 = colonne entièrement nulle.
            agg = df.select([F.count(F.col(f"`{c}`")).alias(c) for c in valides]).first()
            full_null = [c for c in valides if (agg[c] or 0) == 0]
            if full_null:
                logger.warning(
                    "[%s] %d colonne(s) entièrement nulle(s) : %s.",
                    source_label, len(full_null), full_null,
                )
            else:
                logger.info("[%s] aucune colonne entièrement nulle.", source_label)

    return df
