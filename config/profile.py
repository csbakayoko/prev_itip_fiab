"""
Profil de run — périmètre traité.

v2.0 : tout le périmètre est chargé sans filtre clause ; les métriques restent
ventilées par CLAUSE × TYPE_CLAUSE (portées par les données). SODEXO (clause
121981, type PB) était le use case d'origine, désormais généralisé : pour le
rejouer seul, remettre CLIENT_CLAUSES = ["121981"] / CLIENT_TYPE_CLAUSES = ["PB"].
"""

import os

# ── Racine DBFS de travail (sources uploadées, checkpoints, exports) ─────────
# Surcharge sans toucher au code : variable d'environnement ITIP_DBFS_HOME
# (à poser dans la conf du cluster / du Job pour un autre espace de travail).
_DBFS_HOME = os.environ.get(
    "ITIP_DBFS_HOME",
    "dbfs:/FileStore/shared_uploads/cheickseko.bakayoko@axa.fr",
)

# ── Inventaires connus — SOURCE UNIQUE des dates / visions / chemins MRM ────
# Une entrée par année : date d'inventaire, vision CPT, MRM courant, MRM N+1
# ("" = pas de N+1, pas de récupération tardive). Consommé par :
#   - l'inventaire ACTIF ci-dessous (run direct main.py / itip_fiab_powerbi) ;
#   - les widgets des notebooks (itip_fiab_main, itip_fiab_comparaison).
INVENTAIRES = {
    "2023": {
        "date"  : "31/12/2023",
        "vision": "CC2023",
        "mrm"   : f"{_DBFS_HOME}/MRM_FILES/MRM_Fiab_31_12_23_V3.csv",
        "mrm_n1": f"{_DBFS_HOME}/MRM_FILES/MRM_Fiab_30_06_24.csv",   # 30/06/2024
    },
    "2024": {
        "date"  : "31/12/2024",
        "vision": "CC2024",
        "mrm"   : f"{_DBFS_HOME}/MRM_FILES/MRM_Fiab_31_12_24.csv",   # à déposer sur DBFS
        "mrm_n1": "",
    },
}
ANNEE_INVENTAIRE = "2023"          # inventaire ACTIF (DEV) — les valeurs en dérivent
_inv = INVENTAIRES[ANNEE_INVENTAIRE]

# ── Identité du run ─────────────────────────────────────────────────────────
CLIENT_NAME            = "PERIMETRE_GLOBAL"  # libellé du run (synthèse + noms d'export)
CLIENT_CPT_VISION      = _inv["vision"]      # vision comptable CPT (filtre obligatoire)
DATE_INVENTAIRE        = _inv["date"]        # date d'inventaire. "auto" = max(MRM_D_INVENTAIRE).
CLIENT_MRM_STATUT_INV  = None          # filtre statut inventaire au chargement. None = charge
                                       # OUI+NON (un MRM NON n'est pas remonté à la direction
                                       # financière mais reste mappable). La distinction est
                                       # portée par la colonne MRM_STATUT_INV (exploitable export).

# ── Filtres de périmètre (None = pas de filtre → tout le périmètre) ──────────
# Périmètre PB : aucune clause figée, mais on filtre le TYPE sur PB. Côté MRM
# cela écarte les HPB (qui n'existent pas dans le compte) ; côté CPT c'est un
# garde-fou sur le préfixe CPB_ (le compte ne contient déjà que du PB).
CLIENT_CLAUSES      = None         # numéros sans préfixe ; None = toutes les clauses
CLIENT_TYPE_CLAUSES = ["PB"]       # "PB" / "HPB" ; filtre type (MRM = PB seul)

# ── Chemins source MRM (CSV DBFS) de l'inventaire actif ─────────────────────
# Principal = inventaire de référence. N+1 = inventaire postérieur pour
# récupérer les déclarations tardives parmi les CPT_ONLY. None si absent.
FICHIER_MRM    = _inv["mrm"]
FICHIER_MRM_N1 = _inv["mrm_n1"] or None

# ── Source CPT parquet (prioritaire sur la table Hive si défini et lisible) ──
# Export parquet de la table compte (mêmes colonnes brutes que la table Hive).
# Chargé EN PRIORITÉ par load_cpt_raw pour accélérer / fiabiliser le run ;
# fallback automatique sur la table Hive (db_cfg.cpt_table) si le chemin est
# absent ou illisible. None = lire directement la table Hive.
CPT_PARQUET_PATH = None

# ── Mode d'exécution ────────────────────────────────────────────────────────
# Volumétrie dans les logs : les comptages Spark (.count()) PUREMENT informatifs
# du waterfall (matchs/étape, entrée, union) et de la préparation (imputation,
# dédoublonnage). Chacun déclenche un job Spark. True = trace les volumétries
# (diagnostic riche, recommandé en mise au point) ; False = les SKIP (un job de
# moins par étape) pour les gros périmètres en prod. N'affecte QUE les logs, JAMAIS
# les résultats (les comptages de contrôle/flux de décision restent toujours actifs).
LOG_VOLUMETRIE = True

# Répertoire des checkpoints FIABLES (DBFS). Les checkpoints fiables survivent
# à la perte d'un executor (autoscaling / spot), contrairement à localCheckpoint
# dont les blocs vivent sur les executors (→ CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND
# irrécupérable quand le cluster réduit). None = retomber sur localCheckpoint
# (uniquement pour cluster à taille fixe).
CHECKPOINT_DIR = f"{_DBFS_HOME}/itip_fiab_checkpoints"

# ── Export des analyses (restitution toujours en console ; écriture fichiers) ─
EXPORT_BASE_PATH   = f"{_DBFS_HOME}/itip_fiab_exports"  # racine DBFS des sorties (métriques, graphiques)
EXPORT_ANALYSES    = False        # True = écrit les analyses sur disque (DBFS)
EXPORT_GRAPHS      = True         # True = graphiques de restitution (affichage + PNG DBFS)
EXPORT_FORMATS     = ("excel", "csv", "parquet")  # ⊆ {excel, csv, parquet, json, delta} — Excel privilégié (Power BI)
# Schéma metastore des tables métriques Delta (run de production Power BI).
# Créé automatiquement au premier export (CREATE SCHEMA IF NOT EXISTS).
# Surcharge : variable d'environnement ITIP_DELTA_SCHEMA ("" = pas de Delta),
# ou widget delta_schema du notebook itip_fiab_powerbi.
EXPORT_DELTA_SCHEMA = os.environ.get("ITIP_DELTA_SCHEMA", "hive_metastore.itip_fiab") or None
