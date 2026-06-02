"""
Mappings colonnes brutes (Hive / CSV) → noms canoniques.

Les noms canoniques sont ensuite préfixés CPT_* / MRM_* par prefix_columns()
dans modules/transform.py.
"""

# ── CPT (table Hive) ────────────────────────────────────────────────────────
MAPPING_CPT = {
    "vision"                         : "VISION",
    "clause"                         : "CLAUSE",
    "n_rpp"                          : "RPP",
    "nom_prenom"                     : "NOM_PRENOM",
    "date_de_naissance_de_l_assure"  : "D_NAISSANCE",
    "date_de_l_arret_de_travail"     : "D_SURVENANCE",
    "exercice_de_survenance"         : "EXERCICE",
    "pm_au_31_12"                    : "PM",
    "psap_au_31_12"                  : "PSAP",
    "terme_de_garantie"              : "GARANTIE",
    "date_de_mise_en_invalidite"     : "D_INVALIDITE",
    "categorie_d_invalidite"         : "CATEGORIE_INVALIDITE",
    "etat_de_dossier_a_l_extraction" : "ETAT_DOSSIER",
}

# ── MRM (CSV) ───────────────────────────────────────────────────────────────
MAPPING_MRM = {
    "CONCLUSION_SYNTHESE" : "CONCLUSION",
    "SINISTRE_MRM"        : "SINISTRE_MRM",
    "SINISTRE_DSN"        : "SINISTRE_DSN",
    "IDCORP"              : "IDCORP",
    "IDPMUN"              : "IDPMUN",
    "RPP_JURIDIQUE"       : "RPP",
    "NOM_PRENOM"          : "NOM_PRENOM",
    "DNASS0"              : "D_NAISSANCE",
    "DDSIA4"              : "D_SURVENANCE",
    "CDGRCA"              : "GARANTIE",
    "Statut_INV"          : "STATUT_INV",
    "PM_BASE_Revue_INV"   : "PM",
    "PM_Exo_INV"          : "PM_EXO_INV",
    "PSAP_TOTAL"          : "PSAP",
    "n_clause_ratta1"     : "CLAUSE",
    "TYPE_CLAUSE"         : "TYPE_CLAUSE",
    "IDSIX"               : "NUM_SINISTRE",
    "DTIRDI"             : "D_INVENTAIRE",
    "DDRIAT"              : "D_INVALIDITE",
}

# ── Correspondance TYPE_CLAUSE CPT ↔ MRM ────────────────────────────────────
# CPT : préfixe sur la colonne `clause` (ex: "CPB_121981")
# MRM : valeur séparée dans la colonne TYPE_CLAUSE ("PB" / "HPB")
MRM_TYPE_CLAUSE_COL = "TYPE_CLAUSE"          # nom exact de la colonne dans le CSV brut

TYPE_CLAUSE_CPT_PREFIX = {                   # préfixe CPT par type
    "PB": "CPB_",
    # "HPB": "??_",                          # à définir lors de l'ouverture HPB
}

TYPE_CLAUSE_MRM_VALUE = {                     # valeur MRM par type
    "PB": "PB",
    # "HPB": "HPB",
}
