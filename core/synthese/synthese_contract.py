"""
Contrat typé de la synthèse (sortie de kpi_export.compute_synthese).

POURQUOI : `compute_synthese` renvoie un dict de 75 clés, lu « à la main » par
metrics.py et viz.py (`d["taux_chute_inventaire"]`, …). Une faute de frappe ne se
voyait qu'au runtime (KeyError). Ce TypedDict matérialise le contrat :
    - l'IDE / un type-checker signalent une clé inexistante AVANT le run ;
    - tests/test_synthese_contract.py vérifie que les clés du TypedDict == les clés
      réellement renvoyées par compute_synthese (garde-fou anti-dérive structurelle).

NON-BREAKING : un TypedDict EST un dict au runtime — aucune valeur ni aucun
comportement ne change, c'est une pure annotation. Ce module n'importe PAS pyspark
(typing seul) → testable sans cluster.
"""

from typing import Dict, List
try:                                   # TypedDict natif depuis 3.8 ; fallback prudent
    from typing import TypedDict
except ImportError:                    # pragma: no cover
    from typing_extensions import TypedDict


class ConsigneStats(TypedDict):
    """Bloc renvoyé pour une consigne (KEEP/ADD/STUDY/DELETE) dans `consignes`."""
    nb: int                # dossiers de la consigne (conformité)
    conf: int              # conformes
    pct: float             # % conformité
    ko: int                # non conformes (nommés par le fait : non retrouvé / encore au compte)
    ko_label: str
    nb_match: int          # base PM / chute (matchés inventaire courant)
    nz: int                # PM MRM ≠ 0
    pct_nz: float
    nz0: int               # PM MRM nulle
    pct_nz0: float
    pm_mrm: float
    pm_cpt: float
    delta: float           # PM MRM − PM CPT
    taux_chute: float
    pertinent: bool        # False pour « à supprimer » (PM non pertinente)


class SyntheseScalars(TypedDict):
    """Tous les scalaires de la synthèse (sortie de compute_synthese).

    Une clé ici ⇔ une clé du `return` de compute_synthese (vérifié par le test
    de contrat). Modifier l'un SANS l'autre fait échouer le test.
    """
    # ── Bulle MRM ──
    mrm_nb: int
    mrm_pm: float
    a_supprimer_nb: int
    a_supprimer_pm: float
    a_comparer_nb: int
    a_comparer_pm: float
    principale_nb: int
    principale_pm: float
    affinee_nb: int
    affinee_pm_mrm: float
    recup_nb: int
    recup_pm_mrm: float
    clause_nb: int
    clause_pm: float
    non_mappes_nb: int
    non_mappes_pm: float
    keep_nb: int
    keep_pm: float
    study_nb: int
    study_pm: float
    add_nb: int
    add_pm: float
    # ── Bulle MATCHÉS ──
    match_nb: int
    match_pm_mrm: float
    match_pm_cpt: float
    match_pm_ecart: float
    # ── Bulle COMPTE ──
    cpt_nb: int
    cpt_pm: float
    trouves_nb: int
    trouves_pm_mrm: float
    trouves_pm_cpt: float
    late_nb: int
    late_pm: float
    late_pm_mrm: float
    late_pm_cpt: float
    obs_nb: int
    obs_pm: float
    recup_non_nb: int
    recup_non_pm: float
    recup_non_n_nb: int
    recup_non_n_pm: float
    recup_non_n1_nb: int
    recup_non_n1_pm: float
    recup_non_pm_mrm: float
    recup_non_pm_mrm_nz: int
    recup_non_pm_mrm_ok: bool
    def_nb: int
    def_pm: float
    # ── Indicateurs (taux) ──
    taux_couverture_mrm: float
    taux_couverture_compte: float
    taux_recup_tardive: float
    taux_recup_global: float
    taux_chute_inventaire: float
    taux_chute_consignes: float
    chute_coherente: bool
    conformite_globale: float
    # ── Niveaux de PM (univers du taux de chute) ──
    metrics_pm_mrm: float
    metrics_pm_cpt: float
    metrics_pm_ecart: float
    metrics_nb: int
    hors_consigne_nb: int
    hors_consigne_pm_mrm: float
    hors_consigne_pm_cpt: float
    # ── Récupérés N+1 (analyse séparée) ──
    chute_n1_nb: int
    chute_n1_pm_mrm: float
    chute_n1_pm_cpt: float
    taux_chute_n1: float
    n1_consignes: Dict[str, int]
    n1_sans_consigne: int
    # ── Suivi des consignes (détail) ──
    consignes: Dict[str, ConsigneStats]
    # ── Invariant de cohérence ──
    total_rows: int
    classified_rows: int
    coherent: bool
    labels_inconnus: List[str]
    # ── Entête ──
    date_inventaire: str
