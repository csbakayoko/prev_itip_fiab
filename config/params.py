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

WINDOW_DAYS = 14                             # tolérance ±jours pour les étapes "window"

# Regroupement pour la synthèse SODEXO :
#   principale   = clé nominale complète (date jour ou ±WINDOW_DAYS)
#   affinée      = clé nominale tronquée CPT 20 chars
#   récupération = matchs métier fiables (passage IP, rechute)
#   clause       = clé de secours (n° de clause à la place du RPP) — rattrape les
#                  dossiers dont le RPP compte est nul / mal renseigné, qui
#                  finissaient en CPT_ONLY définitif faute de clé exploitable.
MATCH_PRINCIPALE   = ("MATCH_EXACT", "MATCH_WINDOW")
MATCH_AFFINEE      = ("MATCH_TRONC", "MATCH_TRONC_WINDOW")
MATCH_RECUPERATION = ("MATCH_IP", "MATCH_RECHUTE", "MATCH_RECHUTE_TRONC")
MATCH_CLAUSE       = ("MATCH_CLAUSE", "MATCH_CLAUSE_WINDOW",
                      "MATCH_CLAUSE_TRONC", "MATCH_CLAUSE_TRONC_WINDOW")

# Matchs LÉGITIMES posés par le waterfall. Sert de référence "matché" partout
# (synthèse, audit consignes, taux de chute). Les matchs sur clé clause ont une
# vraie contrepartie MRM → ils entrent dans l'univers de chute comme les autres.
MATCH_LABELS = MATCH_PRINCIPALE + MATCH_AFFINEE + MATCH_RECUPERATION + MATCH_CLAUSE


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

# Déclarations tardives IT : un CPT_ONLY resté orphelin (absent du MRM courant ET
# du N+1, donc « pas dans deux exercices successifs ») dont la survenance tombe en
# fin d'année (mois ∈ ORPHAN_FIN_ANNEE_MOIS) et dont la garantie vaut LATE_IT_GARANTIE
# (incapacité de travail) est calé en CPT_LATE : observation tardive d'un IT dont la
# couverture a vraisemblablement pris fin avant l'inventaire de l'exercice suivant.
LATE_IT_GARANTIE      = 60

# Label des observations tardives IT. Ces dossiers (garantie 60, survenance fin
# d'année) n'apparaissent ni dans le MRM courant ni dans le N+1 : le sinistre a
# vraisemblablement eu lieu ET s'est CLOS avant la date d'inventaire du MRM suivant
# — il est donc LOGIQUE de ne pas les retrouver. Ce ne sont PAS des anomalies (≠
# CPT_ONLY définitifs) : on les tague à part, on les EXCLUT des calculs PM / taux
# de chute et des taux de couverture/récupération, mais on présente leur PM compte
# et leur volumétrie. Un label distinct de CPT_LATE garantit l'exclusion par
# construction (tout code basé sur MATCH_LABELS / CPT_LATE les ignore).
OBS_TARDIVE_LABEL     = "CPT_OBS_TARDIVE"

# Label des CPT_ONLY récupérés via un MRM au statut inventaire NON. Le statut NON
# n'alimente PAS le matching principal (réalisé sur les OUI seuls) : il sert
# uniquement à une passe de repêchage des CPT_ONLY restants. Un CPT ainsi récupéré
# prouve qu'une contrepartie MRM existe (statut NON, donc PM MRM = 0, non remontée
# à la direction financière) → on le tague à part. Label distinct ⇒ EXCLU par
# construction de TOUTES les métriques (couverture, chute, conformité) ; présenté
# uniquement dans son analyse dédiée. LATE_SOURCE = "STATUT_NON".
RECUP_NON_LABEL       = "CPT_RECUP_NON"

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
