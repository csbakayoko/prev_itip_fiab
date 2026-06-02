"""
Paramètres techniques du pipeline de fiabilisation.

Matching en cascade, clés de dédoublonnage, source base de données.
La config est immuable au runtime (dataclasses gelées).
"""

from dataclasses import dataclass
from typing import Tuple


# ============================================================================
# MATCHING WATERFALL
# ============================================================================

WINDOW_DAYS = 7                              # tolérance ±jours pour les étapes "window"

# Regroupement pour la synthèse SODEXO :
#   principale   = clé nominale complète (date jour ou ±WINDOW_DAYS)
#   affinée      = clé nominale tronquée CPT 20 chars
#   récupération = matchs métier de rattrapage (IP, rechute, date en retard)
MATCH_PRINCIPALE   = ("MATCH_EXACT", "MATCH_WINDOW")
MATCH_AFFINEE      = ("MATCH_TRONC", "MATCH_TRONC_WINDOW")
MATCH_RECUPERATION = ("MATCH_IP", "MATCH_RECHUTE", "MATCH_RECHUTE_TRONC", "MATCH_DATE_RETARD")

# Labels posés dans TYPE_RECONCILIATION par le waterfall (ordre = cascade).
# = tous les matchs confondus, dérivés des 3 sous-catégories ci-dessus.
MATCH_LABELS = MATCH_PRINCIPALE + MATCH_AFFINEE + MATCH_RECUPERATION


# ============================================================================
# PASSAGE IT → IP
# ============================================================================
# Étape de récupération des orphelins : on rejoue le matching sur une clé sans
# la garantie (rpp + dob + survenance exacte + nom), puis on valide par l'écart
# de code garantie. Un passage incapacité (IT) → invalidité (IP) se traduit par
#   garantie_CPT − garantie_MRM == IP_GARANTIE_OFFSET
# Sinon le rapprochement est cassé (faux positif) → les dossiers restent orphelins.
# Mettre None pour désactiver complètement l'étape IP.
IP_GARANTIE_OFFSET   = 4
RELAPSE_WINDOW_DAYS  = 30   # fenêtre max (jours) pour rattacher une rechute IT


# ============================================================================
# ENTREPÔT DE DONNÉES (investigations orphelins)
# ============================================================================
# Lecture des gros fichiers Excel de l'entrepôt via spark-excel
# (com.crealytics.spark.excel, à installer sur le cluster Databricks).

EXCEL_FORMAT = "com.crealytics.spark.excel"
EXCEL_SHEET  = None   # None = 1re feuille ; sinon nom d'onglet (ex: "Feuil1")
EXCEL_DATE_FORMAT = "dd/MM/yyyy"   # format des dates dans l'Excel entrepôt


# ============================================================================
# BASE DE DONNÉES
# ============================================================================

@dataclass(frozen=True)
class DatabaseConfig:
    cpt_table     : str
    mrm_delimiter : str


db_cfg = DatabaseConfig(
    cpt_table     = "hive_metastore.compteclient.tetepartete_itip",
    mrm_delimiter = ";",
)


# ============================================================================
# NETTOYAGE / DÉDOUBLONNAGE
# ============================================================================

@dataclass(frozen=True)
class TechnicalConfig:
    cpt_order_col : str                      # colonne de tri pour le pick last-write CPT
    cpt_dup_keys  : Tuple[str, ...]          # clés de dédoublonnage CPT (colonnes brutes)
    mrm_dup_keys  : Tuple[str, ...]          # clés de dédoublonnage MRM (colonnes après rename)


tech_cfg = TechnicalConfig(
    cpt_order_col = "tech_day",
    cpt_dup_keys  = (
        "n_rpp",
        "exercice_de_survenance",
        "nom_prenom",
        "date_de_naissance_de_l_assure",
        "date_de_l_arret_de_travail",
        "terme_de_garantie",
    ),
    mrm_dup_keys  = (
        "IDCORP",
        "D_NAISSANCE",
        "D_SURVENANCE",
        "GARANTIE",
    ),
)
