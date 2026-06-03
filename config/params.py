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
#   récupération = matchs métier fiables (passage IP, rechute)
MATCH_PRINCIPALE   = ("MATCH_EXACT", "MATCH_WINDOW")
MATCH_AFFINEE      = ("MATCH_TRONC", "MATCH_TRONC_WINDOW")
MATCH_RECUPERATION = ("MATCH_IP", "MATCH_RECHUTE", "MATCH_RECHUTE_TRONC")

# Anomalies : MATCH_DATE_RETARD est un appariement à risque (faux positif
# possible). Exclu des matchs légitimes → exclu des calculs de PM et du suivi
# des consignes. Reporté à part avec l'écart de jours entre survenances.
MATCH_ANOMALIE     = ("MATCH_DATE_RETARD",)

# Matchs LÉGITIMES posés par le waterfall (anomalies exclues). Sert de référence
# "matché" partout (synthèse, audit consignes, taux de chute).
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
RELAPSE_WINDOW_DAYS  = 30    # fenêtre max (jours) pour rattacher une rechute IT
RETARD_MAX_DAYS      = 365   # plafond (jours) pour MATCH_DATE_RETARD : au-delà,
                             # l'écart de date traduit deux sinistres distincts,
                             # pas une erreur de saisie → on n'apparie plus.

# Segmentation des orphelins CPT_ONLY définitifs (tag TAG_CPT_ONLY) :
#   - fin d'année d'inventaire (mois ∈ ORPHAN_FIN_ANNEE_MOIS) → tardif probable
#   - PM > ORPHAN_PM_THRESHOLD                                → orphelin montant élevé
#   - sinon                                                   → à analyser
ORPHAN_PM_THRESHOLD   = 20_000
ORPHAN_FIN_ANNEE_MOIS = (11, 12)


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
