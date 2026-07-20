"""
Couche métriques — calcul des données de restitution depuis df_result.

FAÇADE du paquet : ré-exporte l'API des modules thématiques, pour que
`from core import metrics` puis `metrics.xxx` fonctionne partout
(main, notebooks, viz, tests) sans connaître le découpage interne :

    base.py       — helpers partagés (exercices, blocs d'ancienneté,
                    dimensions CLAUSE / TYPE_COMPTE, univers de chute,
                    chemins DBFS)
    scalaires.py  — les métriques depuis `d` (dict de compute_synthese) :
                    dim_run, synthese, bilan_cas, consignes, couverture
    agregats.py   — les ré-agrégations Spark de df_result et les tables
                    regroupées `chute` / `orphelins` (angles empilés par AXE)
    coherence.py  — controles_coherence (recoupements inter-tables)
    export.py     — toutes_metriques + export_metriques (multi-format)
    viz.py        — les 12 graphiques matplotlib (import séparé)

Les fonctions renvoient des DONNÉES BRUTES (nombres, pas de chaînes
formatées) : le formatage (M€, %, séparateurs FR) reste au niveau
restitution. Contrat des métriques : docs/METRIQUES.md.

LES 9 TABLES EXPORTÉES (toutes_metriques) — une table = un sujet complet,
les angles d'analyse sont des colonnes (EXERCICE, AXE, SEGMENT, UNIVERS) :
    dim_run (la dimension de run, pivot du modèle en étoile — reliée à
    chaque table par la clé CLE_RUN posée à l'export), synthese, bilan_cas,
    couverture, chute, consignes, consignes_par_type_compte, orphelins,
    controles_coherence.

Correspondance avec les 12 graphiques (core.metrics.viz) — les graphes se
nourrissent de `d` et des ré-agrégations par axe (briques des tables) :
    1. compte_justification   → couverture(d), univers Compte
    2. couverture_mrm         → couverture(d), univers Revue MRM
    3. chute_par_type_compte  → chute_par_type_compte(df_result)  [× exercice]
    4. chute_par_consigne     → consignes(d), exercice courant
    5. conformite_consignes   → consignes(d), exercice courant
    6. anomalies_cpt_only     → anomalies_cpt_only(df_result)
    7. kpi_chute              → chute(df_result, d), axe Ensemble
    8. kpi_conformite_globale → consignes(d) + d["conformite_globale"]
    9. pm_par_consigne        → consignes(d), exercice courant
   10. chute_par_anciennete   → chute_par_anciennete(df_result, annee)  [× exercice]
   11. orphelins_par_compte   → orphelins_par_clause(df_result)
   12. distribution_ecarts    → chute_par_tranche_ecart(df_result)  [× exercice]

AXE D'ANALYSE : les métriques se ventilent par TYPE_COMPTE (PB / HPB / …), le
périmètre métier. La CLAUSE n'est PAS un axe : c'est un substitut du RPP dans
la clé de matching, et tous les types de compte n'en portent pas. Elle ne
subsiste que dans `orphelins_par_clause`, table de DÉTAIL d'investigation
(sous-ensemble : uniquement les dossiers porteurs d'une clause).

Usage (main / notebook) :
    from core import metrics

    d = print_synthese(df_result)              # la passe Spark
    metrics.bilan_cas(d)                       # le bilan cas par cas
    metrics.chute_par_type_compte(df_result)   # ré-agrégation Spark → pandas
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
    dim_run,
    synthese,
    bilan_cas,
    consignes,
    couverture,
    SANS_CONSIGNE,
    UNIVERS_COMPTE,
    UNIVERS_REVUE,
)
from core.metrics.agregats import (
    chute,
    orphelins,
    chute_par_type_compte,
    chute_par_anciennete,
    chute_par_tranche_ecart,
    tranches_ecart,
    anomalies_cpt_only,
    consignes_par_type_compte,
    orphelins_par_type_compte,
    orphelins_par_clause,
    orphelins_par_garantie,
    orphelins_par_anciennete,
    orphelins_cles_nulles,
    AXE_ENSEMBLE,
    AXE_TYPE_COMPTE,
    AXE_ANCIENNETE,
    AXE_TRANCHE_ECART,
    AXE_GARANTIE,
    AXE_MOIS,
    AXE_CLAUSE,
    AXE_CLE_NULLE,
    TRANCHE_ECART_NUL,
    _assemble_chute,
    _assemble_orphelins,
    _finalise_chute_par_type_compte,
    _finalise_chute_par_anciennete,
    _finalise_chute_par_tranche_ecart,
    _finalise_consignes_par_type_compte,
    _finalise_orphelins,
)
from core.metrics.coherence import controles_coherence
from core.metrics.export import toutes_metriques, export_metriques
