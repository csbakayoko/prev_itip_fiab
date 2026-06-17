"""Tests de la dérivation des scalaires de synthèse — PURE PYTHON, SANS Spark.

C'est le gain du découpage #1 : la logique métier (75 scalaires) est testable
en local avec de simples dicts (les lignes agrégées), sans cluster. On couvre
matchés / non retrouvés / anomalies / récupérés N+1 / obs tardives / repêchés NON.
"""

from core.synthese_scalars import _scalars_from_rows


def _row(type_rec, action=None, source=None, non=False,
         nb=1, pm_mrm=0.0, pm_cpt=0.0, nz_mrm=0, nz_cpt=0):
    """Une ligne agrégée comme la renvoie _collect_rows (lecture par r["clé"])."""
    return {
        "TYPE_RECONCILIATION": type_rec,
        "MRM_ACTION": action,
        "LATE_SOURCE": source,
        "IS_STATUT_NON": non,
        "nb": nb, "pm_mrm": pm_mrm, "pm_cpt": pm_cpt,
        "nb_pm_mrm_nz": nz_mrm, "nb_pm_cpt_nz": nz_cpt,
    }


def _scenario():
    """Un mini df_result couvrant chaque grande catégorie."""
    return [
        _row("MATCH_EXACT", "MRM_KEEP",   pm_mrm=100.0, pm_cpt=80.0, nz_mrm=1, nz_cpt=1),
        _row("MRM_MISSING", "MRM_KEEP",   pm_mrm=50.0,  nz_mrm=1),
        _row("CPT_ONLY",    None,         pm_cpt=30.0,  nz_cpt=1),
        _row("MRM_DELETE",  "MRM_DELETE", pm_mrm=10.0,  nz_mrm=1),
        _row("CPT_LATE",    "MRM_KEEP",   source="MRM_N1",       pm_mrm=60.0, pm_cpt=55.0, nz_mrm=1, nz_cpt=1),
        _row("CPT_OBS_TARDIVE", None,     source="OBS_TARDIVE_IT", pm_cpt=20.0, nz_cpt=1),
        _row("CPT_RECUP_NON",   None,     source="STATUT_NON", non=True, pm_cpt=15.0, nz_cpt=1),
    ]


def test_volumetrie_par_categorie():
    d = _scalars_from_rows(_scenario(), "31/12/2023")
    assert d["match_nb"] == 1
    assert d["non_mappes_nb"] == 1
    assert d["def_nb"] == 1
    assert d["a_supprimer_nb"] == 1
    assert d["late_nb"] == 1
    assert d["obs_nb"] == 1
    assert d["recup_non_nb"] == 1
    assert d["recup_non_n_nb"] == 1          # LATE_SOURCE = STATUT_NON (exercice N)
    assert d["recup_non_n1_nb"] == 0
    assert d["cpt_nb"] == 5                   # match + def + late + obs + recup_non
    assert d["date_inventaire"] == "31/12/2023"


def test_taux_de_chute_et_coherence():
    d = _scalars_from_rows(_scenario(), "31/12/2023")
    # base de chute = le seul matché hors « à supprimer » / NON : (100−80)/100 = 20 %
    assert d["taux_chute_inventaire"] == 20.0
    assert d["metrics_pm_mrm"] == 100.0
    assert d["metrics_pm_cpt"] == 80.0
    assert d["metrics_pm_ecart"] == 20.0
    # N+1 séparé : (60−55)/60 = 8,3 %
    assert d["taux_chute_n1"] == 8.3
    # auto-contrôle interne
    assert d["chute_coherente"] is True


def test_taux_couverture_recup_conformite():
    d = _scalars_from_rows(_scenario(), "31/12/2023")
    assert d["taux_couverture_mrm"] == 50.0       # 1 / (1 matché + 1 missing)
    assert d["taux_couverture_compte"] == 33.3    # 1 / (1 + 1 late + 1 def)
    assert d["taux_recup_tardive"] == 50.0        # 1 late / (1 late + 1 def)
    assert d["taux_recup_global"] == 66.7         # (1 + 1) / 3
    assert d["conformite_globale"] == 50.0        # 1 conforme KEEP / 2 KAS


def test_invariant_coherent_et_consignes():
    d = _scalars_from_rows(_scenario(), "31/12/2023")
    assert d["coherent"] is True
    assert d["labels_inconnus"] == []
    assert d["recup_non_pm_mrm_ok"] is True       # repêchés NON : PM MRM = 0
    cons = d["consignes"]["À conserver"]
    assert cons["taux_chute"] == 20.0
    assert cons["pertinent"] is True
    assert d["consignes"]["À supprimer"]["pertinent"] is False
    assert d["n1_consignes"]["À conserver"] == 1


def test_label_inconnu_casse_la_coherence():
    rows = [
        _row("MATCH_EXACT", "MRM_KEEP", pm_mrm=10.0, pm_cpt=10.0),
        _row("LABEL_BIDON", None, nb=3),
    ]
    d = _scalars_from_rows(rows, "31/12/2023")
    assert d["coherent"] is False                 # 1 classé < 4 total
    assert d["labels_inconnus"] == ["LABEL_BIDON"]
