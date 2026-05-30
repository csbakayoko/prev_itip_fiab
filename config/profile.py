"""
Profil client — périmètre du run en cours.

Un seul client actif à la fois. Pour changer de périmètre, éditer ce fichier.
SODEXO = clause 121981, type PB, vision CC2023.
"""

# ── Identité du client ──────────────────────────────────────────────────────
CLIENT_NAME       = "SODEXO"      # libellé affiché dans la synthèse
CLIENT_CPT_VISION = "CC2023"      # vision comptable CPT (filtre obligatoire)

# "auto" → max(MRM_D_INVENTAIRE) calculé au run ; ou date figée "31/12/2023"
DATE_INVENTAIRE = "auto"

# ── Filtres de périmètre (None = pas de filtre) ─────────────────────────────
CLIENT_CLAUSES      = ["121981"]  # numéros sans préfixe
CLIENT_TYPE_CLAUSES = ["PB"]      # "PB" / "HPB"

# ── Chemins source MRM (CSV DBFS) ───────────────────────────────────────────
# Principal = inventaire de référence (ex: 31/12/2023). N+1, N+2 = inventaires
# postérieurs (ex: 30/06/2024, 31/12/2024) pour récupérer les déclarations
# tardives parmi les CPT_ONLY, en cascade (N+1 puis N+2). None si absent.
FICHIER_MRM    = "dbfs:/FileStore/shared_uploads/cheickseko.bakayoko@axa.fr/MRM_Fiab_31_12_23_V3.csv"
FICHIER_MRM_N1 = None             # MRM N+1 (ex: 30/06/2024)
FICHIER_MRM_N2 = None             # MRM N+2 (ex: 31/12/2024)

# ── Mode d'exécution ────────────────────────────────────────────────────────
DEV_MODE = True                   # False en prod (Job Databricks remplit RUN_PARAMS)
