"""
Couche métriques — calcul des données de restitution depuis df_result.

FAÇADE du paquet : ré-exporte l'API des modules thématiques, pour que
`from core import metrics` puis `metrics.xxx` fonctionne partout
(main, notebooks, viz, tests) sans connaître le découpage interne :

    base.py       — helpers partagés (exercices, blocs d'ancienneté,
                    dérivation CLAUSE, univers de chute, chemins DBFS)
    scalaires.py  — les métriques depuis `d` (dict de compute_synthese) :
                    synthese, bilan_cas, taux_chute, consignes, …
    agregats.py   — les ré-agrégations Spark de df_result : chute par
                    clause / ancienneté, anomalies, orphelins CPT_ONLY
    coherence.py  — controles_coherence (recoupements inter-tables)
    export.py     — toutes_metriques + export_metriques (multi-format)
    viz.py        — les 11 graphiques matplotlib (import séparé)

Les fonctions renvoient des DONNÉES BRUTES (nombres, pas de chaînes
formatées) : le formatage (M€, %, séparateurs FR) reste au niveau
restitution. Contrat des métriques : docs/METRIQUES.md.

Correspondance avec les 11 graphiques (core.metrics.viz) :
    1. compte_justification   → compte_justification(d)
    2. couverture_mrm         → couverture_mrm(d)
    3. chute_par_clause       → chute_par_clause(df_result)  [× exercice]
    4. chute_par_consigne     → chute_par_consigne(d)
    5. conformite_consignes   → conformite_consignes(d)
    6. anomalies_cpt_only     → anomalies_cpt_only(df_result)
    7. kpi_chute              → taux_chute(d)
    8. kpi_conformite_globale → conformite_globale(d)
    9. pm_par_consigne        → pm_par_consigne(d)
   10. chute_par_anciennete   → chute_par_anciennete(df_result, annee)  [× exercice]
   11. orphelins_par_compte   → orphelins_par_clause(df_result)

Usage (main / notebook) :
    from core import metrics

    d = print_synthese(df_result)              # la passe Spark
    metrics.bilan_cas(d)                       # le bilan cas par cas
    metrics.chute_par_clause(df_result)        # ré-agrégation Spark → pandas
    metrics.export_metriques(df_result, d)     # tout sur DBFS
"""

from core.metrics.base import (
    _PERIMETRE,
    _to_local,
    output_dir,
    derive_clause_column,
    _annee_inventaire,
    EXERCICE_INV,
    EXERCICE_N1,
    BLOC_N,
    BLOC_N1,
    BLOC_N2_PLUS,
    BLOC_INDET,
)
from core.metrics.scalaires import (
    synthese,
    bilan_cas,
    taux_chute,
    chute_par_exercice,
    suivi_n1,
    consignes,
    compte_justification,
    couverture_mrm,
    conformite_globale,
    chute_par_consigne,
    pm_par_consigne,
    conformite_consignes,
)
from core.metrics.agregats import (
    chute_par_clause,
    chute_par_anciennete,
    anomalies_cpt_only,
    consignes_par_clause,
    orphelins_par_clause,
    orphelins_par_garantie,
    orphelins_par_anciennete,
    orphelins_cles_nulles,
    _finalise_chute_par_clause,
    _finalise_chute_par_anciennete,
    _finalise_consignes_par_clause,
    _finalise_orphelins,
)
from core.metrics.coherence import controles_coherence
from core.metrics.export import toutes_metriques, export_metriques
