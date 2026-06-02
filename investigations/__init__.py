"""
Investigations sur les orphelins de la réconciliation.

Croise les orphelins CPT_ONLY / MRM_MISSING avec l'entrepôt de données (gros
fichiers Excel multi-inventaire) pour tracer leur historique et comprendre
pourquoi ils ne se sont pas appariés.

Brique principale :
    from investigations import investigate
    results = investigate(spark)   # {cpt_traced, cpt_stats, mrm_traced, mrm_stats}
"""

from investigations.orphans import extract_orphans
from investigations.warehouse import load_warehouse_excel, prepare_warehouse
from investigations.analyze import trace_history, history_stats, print_summary
from investigations.run import investigate

__all__ = [
    "extract_orphans",
    "load_warehouse_excel",
    "prepare_warehouse",
    "trace_history",
    "history_stats",
    "print_summary",
    "investigate",
]
