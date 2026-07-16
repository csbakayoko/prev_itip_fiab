"""
Profil de run — périmètre traité, sources, exports.

Tout le périmètre est chargé sans filtre de clause ; les métriques restent
ventilées par CLAUSE × TYPE_COMPTE, dimensions portées par les données.
Pour restreindre le run à une seule clause (ex. SODEXO), poser
CLIENT_CLAUSES = ["121981"] et CLIENT_TYPE_CLAUSES = ["PB"].
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
# Les fichiers MRM sont déposés à la main sur DBFS (cf. SHAREPOINT plus bas) :
# une nouvelle année = déposer le fichier, puis ajouter son entrée ici.
# Le .csv comme le .xlsx sont acceptés — le format est déduit de l'extension.
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
ANNEE_INVENTAIRE = "2023"          # inventaire ACTIF par défaut — les valeurs ci-dessous en dérivent
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

# Libellé de périmètre du run : la clause si le run est filtré sur une seule,
# sinon "MULTI". Nomme les dossiers/fichiers d'export et alimente la colonne
# PERIMETRE du schéma standard des tables Delta — la clé d'historisation est
# (DATE_INVENTAIRE, PERIMETRE) : les noms de tables, eux, restent STABLES
# (pas de suffixe de périmètre qui casserait les connexions Power BI quand la
# config change).
PERIMETRE_LABEL = CLIENT_CLAUSES[0] if (CLIENT_CLAUSES and len(CLIENT_CLAUSES) == 1) else "MULTI"

# ── Chemins source MRM (CSV DBFS) de l'inventaire actif ─────────────────────
# Principal = inventaire de référence. N+1 = inventaire postérieur pour
# récupérer les déclarations tardives parmi les CPT_ONLY. None si absent.
FICHIER_MRM    = _inv["mrm"]
FICHIER_MRM_N1 = _inv["mrm_n1"] or None

# ── Source SharePoint — DÉSACTIVÉE ───────────────────────────────────────────
# Mode de travail actuel : le fichier MRM est déposé À LA MAIN sur DBFS, et
# INVENTAIRES pointe sur ce chemin dbfs:/ (.csv ou .xlsx, les deux sont lus).
# Aucun appel réseau, aucun secret nécessaire.
#
# "actif": False coupe la voie SharePoint. Un chemin "sharepoint:/..." est
# alors REFUSÉ avec un message explicite plutôt que tenté à moitié — on ne
# télécharge pas silencieusement avec une config incomplète.
#
# Pour réactiver, quand l'app registration Azure AD sera fournie par l'IT :
#   1. renseigner hostname + site (et tenant_id / client_id par variables
#      d'environnement, ou en dur ici si le contexte le permet) ;
#   2. déposer le secret d'app dans le secret scope Databricks — JAMAIS ici ;
#   3. passer "actif" à True ;
#   4. pointer INVENTAIRES sur "sharepoint:/<chemin dans la bibliothèque>".
# Le téléchargement Microsoft Graph vers SHAREPOINT_STAGING reprend alors
# automatiquement avant lecture. Détail des prérequis : core/io/sources.py.
SHAREPOINT = {
    "actif"        : False,              # False = voie SharePoint coupée (dépôt manuel)
    "tenant_id"    : os.environ.get("ITIP_SP_TENANT_ID", ""),
    "client_id"    : os.environ.get("ITIP_SP_CLIENT_ID", ""),
    "client_secret": "",                 # laisser vide : résolu via le secret scope
    "secret_scope" : "itip",             # dbutils.secrets.get(scope, key)
    "secret_key"   : "sp_client_secret",
    "hostname"     : "",                 # ex. "axa.sharepoint.com"
    "site"         : "",                 # ex. "/sites/PrevoyanceITIP"
}
SHAREPOINT_STAGING = f"{_DBFS_HOME}/sharepoint_staging"  # cache DBFS des téléchargements

# ── Source CPT parquet (prioritaire sur la table Hive si défini et lisible) ──
# Export parquet de la table compte (mêmes colonnes brutes que la table Hive).
# Chargé EN PRIORITÉ par load_cpt_raw pour accélérer / fiabiliser le run ;
# fallback automatique sur la table Hive (db_cfg.cpt_table) si le chemin est
# absent ou illisible. None = lire directement la table Hive.
# ⚠ Bien pointer tetepartete_itip.PARQUET (l'export de la table compte) — le
# tetepartete_re.PARQUET voisin est un autre flux (colonnes différentes).
CPT_PARQUET_PATH = "/mnt/lake/compteclient/data/compteclient/tetepartete_re/prepare/tetepartete_itip.PARQUET"

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

# ── Export des analyses ─────────────────────────────────────────────────────
# La restitution console est toujours faite ; ces réglages pilotent l'ÉCRITURE.
EXPORT_BASE_PATH   = f"{_DBFS_HOME}/itip_fiab_exports"  # racine DBFS des sorties (métriques, graphiques)
EXPORT_ANALYSES    = True         # True = écrit les métriques (Delta + fichiers DBFS)
EXPORT_GRAPHS      = True         # True = graphiques de restitution (affichage + PNG DBFS)

# Formats d'écriture, ⊆ {delta, excel, csv, parquet, json}.
# DELTA EN PREMIER : le Hive est la sortie de RÉFÉRENCE (c'est lui que Power BI
# interroge via le SQL Warehouse, et lui seul est historisé par run). Les
# fichiers DBFS (Excel / parquet / csv) sont une sortie SECONDAIRE : dépannage,
# partage ponctuel, import Power BI quand le Warehouse n'est pas disponible.
# Retirer "delta" ici coupe l'écriture Hive sans toucher au reste.
EXPORT_FORMATS     = ("delta", "excel", "parquet", "csv")

# Schéma metastore des tables métriques Delta — la cible de référence.
# Créé automatiquement au premier export (CREATE SCHEMA IF NOT EXISTS).
# Surcharge : variable d'environnement ITIP_DELTA_SCHEMA ("" = pas de Delta),
# ou widget delta_schema du notebook itip_fiab_powerbi.
# ⚠ L'export Delta EXIGE une DATE_INVENTAIRE résoluble (dd/MM/yyyy) : c'est la
# clé d'historisation, on refuse d'écrire à l'aveugle (cf. core/io/save_result).
EXPORT_DELTA_SCHEMA = os.environ.get("ITIP_DELTA_SCHEMA", "hive_metastore.itip_backtest") or None

# Table Delta du DÉTAIL df_result (dans EXPORT_DELTA_SCHEMA), historisée par
# date d'inventaire : rejouer un inventaire remplace ses lignes, 2023 et 2024
# coexistent — analyses fines Power BI au-delà des tables métriques agrégées.
EXPORT_RESULT_TABLE = "resultat_backtest"
