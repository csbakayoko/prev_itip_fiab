"""
Package d'analyse métier de la réconciliation CPT/MRM.

Découpage thématique (un sous-module par famille d'analyse) :
    helpers          — helpers internes + constantes (tranches, identité MRM)
    consignes        — suivi/audit des consignes MRM (+ PM)
    taux_chute       — taux de chute ventilé par clause
    provisionnement  — sous/sur/conforme, écarts par tranches, étude globale
    orphelins        — CPT_ONLY, MRM_MISSING, ventilation, obs tardives IT
    enrich           — tag obs tardives IT + tags orphelins (mutent df_result)
    diagnostics      — diagnostic fan-out CPT/MRM
    export           — restitution console + export multi-format (ventilé par clause)

Toutes les fonctions publiques sont ré-exportées ici → l'import reste
`from modules.analysis import <fonction>` (compat ascendante).
"""

from typing import Dict

from pyspark.sql import DataFrame

from modules.analysis.consignes import (
    analyze_suivi_consignes,
    analyze_consignes_ratios,
    analyze_suivi_consignes_global,
    analyze_consignes_pm,
    analyze_delete_non_suivies,
)
from modules.analysis.taux_chute import (
    calculate_taux_chute,
    analyze_taux_chute,
)
from modules.analysis.provisionnement import (
    analyze_provisionnement,
    analyze_ecarts_tranches,
    study_provisionnement,
    DEFAULT_ECART_TRANCHES,
)
from modules.analysis.orphelins import (
    analyse_cpt_only,
    analyze_mrm_missing,
    ventilate_cpt_only,
    analyze_obs_tardives,
)
from modules.analysis.enrich import (
    flag_late_it_observations,
    enrich_result_tags,
    drop_unmatched_inventory_non,
)
from modules.analysis.diagnostics import diagnose_mrm_fanout
from modules.analysis.helpers import DEFAULT_PM_TRANCHES, derive_clause_column
from modules.analysis.export import (
    tag_clause,
    collect_analyses,
    restituer_analyses,
    export_analyses,
    export_csv,
    export_parquet,
    export_excel,
    export_delta,
)


# ============================================================================
# POINT D'ENTRÉE : RUN FULL ANALYSIS (tables agrégées multi-clause, Power BI)
# ============================================================================

def run_full_analysis(
    df_result     : DataFrame,
    conclusion_col: str = "MRM_CONCLUSION",
    clause_col    : str = "CLAUSE",
) -> Dict[str, DataFrame]:
    """
    Exécute les analyses multi-clause agrégées en une passe (tables Power BI).

    Tables produites :
        synthese_consignes   — audit conformité par (CLAUSE, MRM_ACTION, RESULTAT_AUDIT)
        stats_reconciliation — distribution par (CLAUSE, MRM_ACTION, TYPE_RECONCILIATION)
        taux_chute           — taux de chute par (CLAUSE, MRM_ACTION)
        provisionnement      — catégories par (CLAUSE, MRM_ACTION, CATEGORIE_PROVISION)
        ecarts_tranches      — distribution par (CLAUSE, MRM_ACTION, CATEGORIE, TRANCHE)
        analyse_cpt_only     — CPT_ONLY agrégés par mois + tranche PM
        analyse_mrm_missing  — MRM_MISSING agrégés par mois + tranche PM + consigne

    Les colonnes CLAUSE / TYPE_CLAUSE (absentes du df_result brut : la clause est
    portée par CPT_CLAUSE / MRM_CLAUSE) sont dérivées ici via derive_clause_column.
    Les MRM statut NON non matchés sont jetés en tête (cohérence : ils n'ont aucune
    contrepartie compte). Pour la restitution/export console, voir
    export.collect_analyses.
    """
    # Rejet des NON non matchés (idempotent ; déjà tracé dans main si lancé là).
    df_result = drop_unmatched_inventory_non(df_result, log=False)
    df_result = derive_clause_column(df_result)
    synthese_consignes, _ = analyze_suivi_consignes(df_result, conclusion_col, clause_col)

    return {
        "synthese_consignes"   : synthese_consignes,
        "stats_reconciliation" : analyze_consignes_ratios(df_result, conclusion_col, clause_col),
        "taux_chute"           : calculate_taux_chute(df_result, conclusion_col, clause_col),
        "provisionnement"      : analyze_provisionnement(df_result, conclusion_col, clause_col),
        "ecarts_tranches"      : analyze_ecarts_tranches(df_result, conclusion_col, clause_col=clause_col),
        "analyse_cpt_only"     : analyse_cpt_only(df_result, clause_col),
        "analyse_mrm_missing"  : analyze_mrm_missing(df_result, conclusion_col, clause_col),
    }
