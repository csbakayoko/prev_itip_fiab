"""
Logique métier de réconciliation en cascade (waterfall) — FAÇADE.

Ré-exporte l'API publique des modules du paquet, pour que
`from core.match.matching import X` reste stable partout :

    steps.py     — clés, conditions réutilisables, _materialize,
                   execute_matching_step (une étape de join)
    waterfall.py — consignes MRM (categorize/filter), orphelins,
                   matching_waterfall (la cascade principale)
    recovery.py  — recover_late_declarations (N+1, statut NON),
                   flag_late_it_observations, enrich_result_tags

⚠ Surcharge runtime : la date d'inventaire est LUE dans core.match.recovery —
configurer_run (core/runtime.py) réassigne recovery.DATE_INVENTAIRE.
"""

from core.match.steps import (
    RECOVERY_KEYS,
    execute_matching_step,
)
from core.match.waterfall import (
    categorize_mrm_conclusion,
    filter_mrm_by_action,
    matching_waterfall,
    tag_orphans,
)
from core.match.recovery import (
    derive_clause_column,
    enrich_result_tags,
    flag_late_it_observations,
    recover_late_declarations,
)

__all__ = [
    "RECOVERY_KEYS",
    "execute_matching_step",
    "categorize_mrm_conclusion",
    "filter_mrm_by_action",
    "matching_waterfall",
    "tag_orphans",
    "derive_clause_column",
    "enrich_result_tags",
    "flag_late_it_observations",
    "recover_late_declarations",
]
