"""
Configuration ITIP-FIAB — point d'entrée unique.

Trois fichiers thématiques, ré-exportés à plat pour les modules :
    profile.py   — périmètre du client actif (SODEXO)
    mappings.py  — colonnes brutes → noms canoniques
    params.py    — matching, dédoublonnage, base de données

Usage :
    from config import db_cfg, tech_cfg, MAPPING_CPT, CLIENT_CLAUSES, ...

Pour changer de périmètre : éditer config/profile.py.
"""

import logging

from .profile import (
    CLIENT_NAME,
    CLIENT_CPT_VISION,
    CLIENT_MRM_STATUT_INV,
    DATE_INVENTAIRE,
    CLIENT_CLAUSES,
    CLIENT_TYPE_CLAUSES,
    FICHIER_MRM,
    FICHIER_MRM_N1,
    DEV_MODE,
)
from .mappings import (
    MAPPING_CPT,
    MAPPING_MRM,
    MRM_TYPE_CLAUSE_COL,
    TYPE_CLAUSE_CPT_PREFIX,
    TYPE_CLAUSE_MRM_VALUE,
)
from .params import (
    DatabaseConfig,
    TechnicalConfig,
    db_cfg,
    tech_cfg,
    WINDOW_DAYS,
    MATCH_LABELS,
    MATCH_PRINCIPALE,
    MATCH_AFFINEE,
    MATCH_RECUPERATION,
    IP_GARANTIE_OFFSET,
    RELAPSE_WINDOW_DAYS,
    LATE_IT_GARANTIE,
    OBS_TARDIVE_LABEL,
    ORPHAN_PM_THRESHOLD,
    ORPHAN_FIN_ANNEE_MOIS,
)

logger = logging.getLogger(__name__)


# ============================================================================
# RUN_PARAMS — chemins source du run
# ============================================================================
# En DEV : hydraté depuis profile.py.
# En PROD : le Job Databricks remplit RUN_PARAMS avant l'import des modules.

RUN_PARAMS: dict = {}

if DEV_MODE:
    if FICHIER_MRM:
        RUN_PARAMS.setdefault("fichier_mrm", FICHIER_MRM)
    if FICHIER_MRM_N1:
        RUN_PARAMS.setdefault("fichier_mrm_n1", FICHIER_MRM_N1)


_clauses = ", ".join(CLIENT_CLAUSES)      if CLIENT_CLAUSES      else "TOUTES"
_types   = ", ".join(CLIENT_TYPE_CLAUSES) if CLIENT_TYPE_CLAUSES else "TOUS"
logger.info(
    "Config chargée | mode=%s | client=%s | vision=%s | clauses=%s | types=%s | window=±%dj",
    "DEV" if DEV_MODE else "PROD", CLIENT_NAME, CLIENT_CPT_VISION,
    _clauses, _types, WINDOW_DAYS,
)
