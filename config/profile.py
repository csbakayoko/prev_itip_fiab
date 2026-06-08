"""
Profil de run — périmètre traité.

v2.0 : tout le périmètre est chargé sans filtre clause ; les métriques restent
ventilées par CLAUSE × TYPE_CLAUSE (portées par les données). SODEXO (clause
121981, type PB) était le use case d'origine, désormais généralisé : pour le
rejouer seul, remettre CLIENT_CLAUSES = ["121981"] / CLIENT_TYPE_CLAUSES = ["PB"].
"""

# ── Identité du run ─────────────────────────────────────────────────────────
CLIENT_NAME            = "PERIMETRE_GLOBAL"  # libellé du run (synthèse + noms d'export)
CLIENT_CPT_VISION      = "CC2023"      # vision comptable CPT (filtre obligatoire)
DATE_INVENTAIRE        = "31/12/2023"  # date d'inventaire (en dur). "auto" = max(MRM_D_INVENTAIRE).
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

# ── Chemins source MRM (CSV DBFS) ───────────────────────────────────────────
# Principal = inventaire de référence (ex: 31/12/2023). N+1 = inventaire
# postérieur (ex: 30/06/2024) pour récupérer les déclarations tardives parmi
# les CPT_ONLY. None si absent.
FICHIER_MRM    = "dbfs:/FileStore/shared_uploads/cheickseko.bakayoko@axa.fr/MRM_FILES/MRM_Fiab_31_12_23_V3.csv"
FICHIER_MRM_N1 = None             # MRM N+1 (ex: 30/06/2024)

# ── Mode d'exécution ────────────────────────────────────────────────────────
DEV_MODE = True                   # False en prod (Job Databricks remplit RUN_PARAMS)

# ── Export des analyses (restitution toujours en console ; écriture fichiers) ─
EXPORT_ANALYSES    = False        # True = écrit les analyses sur disque (DBFS)
EXPORT_FORMATS     = ("csv", "parquet", "excel", "json")  # ⊆ {csv, parquet, excel, json, delta}
EXPORT_DELTA_SCHEMA = None        # schéma metastore cible si "delta" ∈ EXPORT_FORMATS
