"""
Couche métriques — calcul des données de restitution depuis df_result.

Sépare le CALCUL des données (ici) de leur RESTITUTION (modules.viz, Excel,
Power BI). Chaque métrique est une méthode qui retourne un objet `Metric`
sérialisable à la demande : DataFrame (pandas ou Spark), JSON, CSV, Parquet.

Principe :
    - compute_synthese(df_result) n'est appelé QU'UNE FOIS (une passe Spark),
      à la construction de `Metrics`. Toutes les métriques scalaires en dérivent.
    - Deux métriques par-axe (chute par clause, anomalies par mois) ré-agrègent
      df_result côté Spark, puis sont matérialisées en pandas.

Les méthodes renvoient des DONNÉES BRUTES (nombres, pas de chaînes formatées) :
le formatage (M€, %, séparateurs FR) reste au niveau restitution.

Correspondance avec les 9 graphiques (modules.viz) :
    1. compte_justification   → compte_justification()
    2. couverture_mrm         → couverture_mrm()
    3. chute_par_clause       → chute_par_clause()
    4. chute_par_consigne     → chute_par_consigne()
    5. conformite_consignes   → conformite_consignes()
    6. anomalies_cpt_only     → anomalies_cpt_only()
    7. kpi_chute_globale      → taux_chute_global()
    8. kpi_conformite_globale → conformite_globale()
    9. pm_par_consigne        → pm_par_consigne()

Usage :
    from modules.metrics import Metrics

    m = Metrics(df_result)                  # une passe Spark
    m.consignes().to_pandas()               # DataFrame pandas
    m.taux_chute_global().to_json()         # str JSON
    m.compte_justification().to_csv(path)   # écrit un CSV
    m.export()                              # toutes les métriques sur DBFS
"""

import os
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F

from config import (
    CLIENT_NAME, CLIENT_CLAUSES, MATCH_LABELS, TYPE_CLAUSE_CPT_PREFIX,
)
from modules.kpi_export import compute_synthese, kas_totaux
from modules.matching import categorize_mrm_conclusion


# ============================================================================
# CHEMINS D'EXPORT (DBFS)
# ============================================================================

# Libellé de périmètre pour nommer les sorties : la clause si le run est
# filtré sur une seule, sinon "MULTI". La clause réelle reste DANS les tables.
_PERIMETRE = CLIENT_CLAUSES[0] if (CLIENT_CLAUSES and len(CLIENT_CLAUSES) == 1) else "MULTI"

DEFAULT_BASE_PATH = (
    "dbfs:/FileStore/shared_uploads/cheickseko.bakayoko@axa.fr/itip_fiab_exports"
)


def _to_local(path: str) -> str:
    """Convertit un chemin dbfs:/... en /dbfs/... pour les writers locaux (pandas)."""
    return path.replace("dbfs:/", "/dbfs/", 1) if path.startswith("dbfs:/") else path


def output_dir(base_path: str = DEFAULT_BASE_PATH, sub: str = "") -> str:
    """Sous-dossier d'export propre au périmètre (<base>/<CLIENT>_<PERIM>[/sub])."""
    out = f"{base_path.rstrip('/')}/{CLIENT_NAME}_{_PERIMETRE}"
    return f"{out}/{sub}" if sub else out


# ============================================================================
# HELPERS SPARK (clause + univers de chute)
# ============================================================================

# Préfixe CPT → type de clause (ex. "CPB" → "PB"). Réciproque de
# TYPE_CLAUSE_CPT_PREFIX, pour dériver le type des dossiers sans contrepartie MRM.
_CPT_PREFIX_TO_TYPE = {v.rstrip("_"): t for t, v in TYPE_CLAUSE_CPT_PREFIX.items()}


def derive_clause_column(df: DataFrame) -> DataFrame:
    """
    Ajoute les colonnes CLAUSE et TYPE_CLAUSE attendues par les agrégations
    par clause. Après le waterfall la clause est portée par CPT_CLAUSE
    (ex. "CPB_121981", préfixe = type) et/ou MRM_CLAUSE (ex. "121981") :

        CLAUSE      = MRM_CLAUSE sinon CPT_CLAUSE sans son préfixe ("CPB_…").
        TYPE_CLAUSE = MRM_TYPE_CLAUSE sinon type déduit du préfixe CPT
                      (CPT_ONLY : pas de MRM → on lit le type dans "CPB_…").
    """
    clause_parts = []
    if "MRM_CLAUSE" in df.columns:
        clause_parts.append(F.col("MRM_CLAUSE"))
    if "CPT_CLAUSE" in df.columns:
        clause_parts.append(F.regexp_replace(F.col("CPT_CLAUSE"), r"^[A-Za-z]+_", ""))
    clause = F.coalesce(*clause_parts) if clause_parts else F.lit(None).cast("string")

    type_parts = []
    if "MRM_TYPE_CLAUSE" in df.columns:
        type_parts.append(F.col("MRM_TYPE_CLAUSE"))
    if "CPT_CLAUSE" in df.columns:
        prefix = F.regexp_extract(F.col("CPT_CLAUSE"), r"^([A-Za-z]+)_", 1)
        type_from_cpt = F.lit(None).cast("string")
        for pfx, t in _CPT_PREFIX_TO_TYPE.items():
            type_from_cpt = F.when(prefix == pfx, F.lit(t)).otherwise(type_from_cpt)
        type_parts.append(type_from_cpt)
    type_clause = F.coalesce(*type_parts) if type_parts else F.lit(None).cast("string")

    return df.withColumn("CLAUSE", clause).withColumn("TYPE_CLAUSE", type_clause)


def _with_mrm_action(df: DataFrame) -> DataFrame:
    """MRM_ACTION persistée par enrich_result_tags ; recalculée si absente."""
    if "MRM_ACTION" in df.columns:
        return df
    return df.withColumn("MRM_ACTION", categorize_mrm_conclusion(F.col("MRM_CONCLUSION")))


def _filter_chute_universe(df: DataFrame) -> DataFrame:
    """Univers UNIQUE du taux de chute : matchés inventaire courant + récupérés
    N+1 (CPT_LATE), consignes KEEP / ADD / STUDY. Garantit la cohérence du taux
    par clause ↔ par consigne ↔ global (cf. docs/METRIQUES.md §4). DELETE exclu
    (la chute n'y a pas de sens) ; CPT_OBS_TARDIVE exclu (jamais matché)."""
    return df.filter(
        F.col("TYPE_RECONCILIATION").isin(list(MATCH_LABELS) + ["CPT_LATE"]) &
        F.col("MRM_ACTION").isin("MRM_KEEP", "MRM_ADD", "MRM_STUDY")
    )


def _mois_label_expr(date_col: str) -> F.Column:
    """Abréviation française du mois (Jan … Déc) depuis une colonne date."""
    labels = ["Jan", "Fév", "Mar", "Avr", "Mai", "Jun",
              "Jul", "Aoû", "Sep", "Oct", "Nov", "Déc"]
    m = F.month(F.col(date_col))
    expr = F.lit("Déc")
    for i, lbl in enumerate(labels[:-1], start=1):
        expr = F.when(m == i, lbl).otherwise(expr)
    return expr


# ============================================================================
# CONTENEUR SÉRIALISABLE
# ============================================================================

@dataclass
class Metric:
    """Une métrique = un nom + une table pandas, sérialisable à la demande.

    to_pandas / to_spark renvoient un DataFrame ; to_json / to_csv renvoient la
    chaîne (path=None) ou écrivent le fichier et renvoient le chemin ; to_parquet
    écrit toujours (format binaire) et renvoie le chemin.
    """

    name : str
    data : pd.DataFrame
    spark: Optional[SparkSession] = None

    def to_pandas(self) -> pd.DataFrame:
        return self.data

    def to_spark(self, spark: Optional[SparkSession] = None) -> DataFrame:
        spark = spark or self.spark
        if spark is None:
            raise ValueError(
                "Aucune SparkSession disponible — passez spark=... ou construisez "
                "Metrics depuis un df_result (sa session est mémorisée)."
            )
        return spark.createDataFrame(self.data)

    def to_json(self, path: Optional[str] = None, orient: str = "records") -> str:
        js = self.data.to_json(orient=orient, force_ascii=False,
                               indent=2, date_format="iso")
        if path is None:
            return js
        with open(_to_local(path), "w", encoding="utf-8") as f:
            f.write(js)
        return path

    def to_csv(self, path: Optional[str] = None, sep: str = ";") -> str:
        if path is None:
            return self.data.to_csv(index=False, sep=sep)
        self.data.to_csv(_to_local(path), index=False, sep=sep, encoding="utf-8")
        return path

    def to_parquet(self, path: str) -> str:
        self.data.to_parquet(_to_local(path), index=False)
        return path


# ============================================================================
# EXTRACTEUR DE MÉTRIQUES
# ============================================================================

class Metrics:
    """Extrait les métriques de restitution depuis un df_result brut.

    compute_synthese (la passe Spark coûteuse) n'est calculée qu'une fois, à la
    construction. Chaque méthode reshape ces scalaires en une table tidy ; deux
    méthodes (chute_par_clause, anomalies_cpt_only) ré-agrègent df_result.

    `d` (scalaires de synthèse) et `k` (totaux KEEP+ADD+STUDY) sont exposés
    pour la couche viz — même passe, mêmes univers, chiffres réconciliables.
    """

    def __init__(self, df_result: DataFrame):
        self._df    = _with_mrm_action(derive_clause_column(df_result))
        self._spark = df_result.sparkSession
        self.d      = compute_synthese(df_result)
        self.k      = kas_totaux(self.d)

    def _metric(self, name: str, rows: list) -> Metric:
        return Metric(name, pd.DataFrame(rows), self._spark)

    # ── 0. Synthèse 1 ligne (KPI historisables) ──────────────────────────────

    def synthese(self) -> Metric:
        """Tous les indicateurs de tête en une ligne (historisable par run)."""
        d, k = self.d, self.k
        cons = d["consignes"]
        return self._metric("synthese", [{
            "DATE_INVENTAIRE"        : d["date_inventaire"],
            "TAUX_CHUTE_GLOBAL_PCT"  : d["taux_chute_global"],
            "CONFORMITE_GLOBALE_PCT" : d["conformite_globale"],
            "TAUX_COUVERTURE_MRM_PCT"    : d["taux_couverture_mrm"],
            "TAUX_COUVERTURE_COMPTE_PCT" : d["taux_couverture_compte"],
            "TAUX_RECUP_TARDIVE_PCT" : d["taux_recup_tardive"],
            "TAUX_RECUP_GLOBAL_PCT"  : d["taux_recup_global"],
            "PM_MRM_BASE_CHUTE"      : k["pm_mrm"],
            "PM_CPT_BASE_CHUTE"      : k["pm_cpt"],
            "ECART_BASE_CHUTE"       : k["delta"],
            "PM_MRM_TOTALE"          : d["mrm_pm"],
            "PM_CPT_TOTALE"          : d["cpt_pm"],
            "NB_MATCHES"             : d["match_nb"],
            "NB_RECUP_N1"            : d["late_nb"],
            "NB_CPT_ONLY"            : d["def_nb"],
            "NB_MRM_MISSING"         : d["non_mappes_nb"],
            "NB_NON_CONFORME"        : cons["À conserver"]["ko"] + cons["À supprimer"]["ko"],
            "NB_NON_RETROUVE"        : cons["À ajouter"]["ko"] + cons["À étudier"]["ko"],
            "COHERENT"               : d["coherent"],
        }])

    # ── 1. Taux de chute global + PM totales (= KPI graphe 7) ─────────────────

    def taux_chute_global(self) -> Metric:
        """Taux de chute global, PM MRM et PM Compte (base chute + totales).

        Base chute = dossiers retrouvés (inventaire + N+1), consignes
        conserver/étudier/ajouter — l'univers de référence du taux de chute.
        PM totales = grands totaux des deux univers d'entrée (MRM, Compte).
        """
        d, k = self.d, self.k
        return self._metric("taux_chute_global", [{
            "TAUX_CHUTE_GLOBAL_PCT" : d["taux_chute_global"],
            "PM_MRM_BASE_CHUTE"     : k["pm_mrm"],
            "PM_CPT_BASE_CHUTE"     : k["pm_cpt"],
            "ECART_BASE_CHUTE"      : k["delta"],
            "PM_MRM_TOTALE"         : d["mrm_pm"],
            "PM_CPT_TOTALE"         : d["cpt_pm"],
            "NB_BASE_CHUTE"         : d["metrics_nb"],
            "NB_INVENTAIRE"         : d["metrics_match_nb"],
            "NB_RECUP_N1"           : d["metrics_late_nb"],
        }])

    # ── 2. Analyse des consignes (conformité + PM + chute, 4 lignes) ──────────

    def consignes(self) -> Metric:
        """Analyse complète par consigne : conformité, PM et taux de chute.

        Une ligne par consigne (conserver / étudier / ajouter / supprimer).
        Couvre à elle seule les graphiques 4 (chute), 5 (conformité) et
        9 (PM revue vs compte) — les méthodes dédiées en sont des vues filtrées.
        """
        rows = []
        for consigne, c in self.d["consignes"].items():
            rows.append({
                "CONSIGNE"        : consigne,
                "NB_TOTAL"        : c["nb"],
                "NB_CONFORMES"    : c["conf"],
                "PCT_CONFORMITE"  : c["pct"],
                "NB_KO"           : c["ko"],
                "NATURE_KO"       : c["ko_label"],     # non conforme | non retrouvé
                "NB_BASE_CHUTE"   : c["nb_match"],
                "NB_INVENTAIRE"   : c["nb_inv"],
                "NB_RECUP_N1"     : c["nb_late"],
                "PM_MRM"          : c["pm_mrm"],
                "PM_CPT"          : c["pm_cpt"],
                "ECART"           : c["delta"],
                "TAUX_CHUTE_PCT"  : c["taux_chute"],
                "PM_PERTINENTE"   : c["pertinent"],    # False pour « à supprimer »
            })
        return self._metric("consignes", rows)

    # ── 3. Justification du compte client (= graphe 1) ────────────────────────

    def compte_justification(self) -> Metric:
        """Décomposition du compte : retrouvés, récupérés N+1, anomalies.

        Une ligne par catégorie (nb + PM compte), avec son poids dans le compte.
        """
        d = self.d
        cats = [
            ("Retrouvés (inventaire)",    d["match_nb"],     d["match_pm_cpt"]),
            ("Retrouvés via N+1",         d["late_nb"],      d["late_pm"]),
            ("Repêchés (statut MRM non)", d["recup_non_nb"], d["recup_non_pm"]),
            ("Clos avant inventaire N+1", d["obs_nb"],       d["obs_pm"]),
            ("Sans contrepartie (anom.)", d["def_nb"],       d["def_pm"]),
        ]
        tot_nb = sum(c[1] for c in cats) or 1
        tot_pm = sum(c[2] for c in cats) or 1.0
        rows = [{
            "CATEGORIE"   : lbl,
            "NB_DOSSIERS" : nb,
            "PM_CPT"      : pm,
            "PCT_NB"      : round(nb / tot_nb * 100, 1),
            "PCT_PM"      : round(pm / tot_pm * 100, 1),
        } for lbl, nb, pm in cats]
        return self._metric("compte_justification", rows)

    # ── 4. Couverture de la revue MRM (= graphe 2) ────────────────────────────

    def couverture_mrm(self) -> Metric:
        """Part de la revue MRM retrouvée au compte, et non retrouvés par consigne.

        Inclut les « à supprimer » retrouvées au compte (consigne non suivie).
        PCT = part de la revue à comparer (base), sauf « à supprimer » (part de
        sa propre consigne).
        """
        d = self.d
        base  = d["a_comparer_nb"] or 1
        c_del = d["consignes"]["À supprimer"]
        del_ko = c_del["nb"] - c_del["conf"]
        rows = [
            ("Retrouvés au compte",                  d["match_nb"], round(d["match_nb"] / base * 100, 1), None),
            ("À conserver non retrouvé (non conf.)", d["keep_nb"],  round(d["keep_nb"]  / base * 100, 1), d["keep_pm"]),
            ("À étudier non retrouvé",               d["study_nb"], round(d["study_nb"] / base * 100, 1), d["study_pm"]),
            ("À ajouter non retrouvé",               d["add_nb"],   round(d["add_nb"]   / base * 100, 1), d["add_pm"]),
            ("« À supprimer » retrouvées au compte", del_ko,        round(del_ko / (c_del["nb"] or 1) * 100, 1), c_del["pm_mrm"]),
        ]
        return self._metric("couverture_mrm", [{
            "CATEGORIE"   : lbl,
            "NB_DOSSIERS" : nb,
            "PCT"         : pct,
            "PM_MRM"      : pm,
        } for lbl, nb, pct, pm in rows])

    # ── 5. Taux de chute par clause (= graphe 3) — ré-agrège Spark ────────────

    def chute_par_clause(self, top: Optional[int] = None) -> Metric:
        """Taux de chute par clause (KEEP/ADD/STUDY confondues), trié par PM MRM.

        Même univers et même formule agrégée que le taux de chute global :
        Σ des lignes (Σ écart / Σ PM MRM) redonne le taux de chute global.
        top=N → ne garde que les N clauses de plus forte PM MRM.
        """
        df = (
            _filter_chute_universe(self._df)
            .withColumn("_ecart", F.coalesce(F.col("MRM_PM"), F.lit(0.0))
                                - F.coalesce(F.col("CPT_PM"), F.lit(0.0)))
        )
        agg = (
            df.groupBy("CLAUSE", "TYPE_CLAUSE")
            .agg(
                F.count("*").alias("nb_dossiers"),
                F.sum(F.when(F.col("_ecart") > 0, 1).otherwise(0)).alias("nb_sous"),
                F.sum(F.when(F.col("_ecart") < 0, 1).otherwise(0)).alias("nb_sur"),
                F.sum(F.when(F.col("_ecart") == 0, 1).otherwise(0)).alias("nb_conforme"),
                F.round(F.sum("MRM_PM"), 2).alias("pm_mrm"),
                F.round(F.sum("CPT_PM"), 2).alias("pm_cpt"),
                F.round(F.sum("_ecart"), 2).alias("ecart_signe"),
            )
            .withColumn("taux_chute_pct",
                F.round(F.when(F.col("pm_mrm") != 0,
                               F.col("ecart_signe") / F.col("pm_mrm") * 100).otherwise(0.0), 2))
            # Poids de la clause dans la PM MRM totale : le global est la moyenne
            # PONDÉRÉE des taux par clause (pas leur somme).
            .withColumn("poids_pm_pct",
                F.round(F.col("pm_mrm") / F.sum("pm_mrm").over(Window.partitionBy()) * 100, 2))
        )
        pdf = (
            agg.toPandas()
            .sort_values("pm_mrm", ascending=False)
            .reset_index(drop=True)
        )
        if top:
            pdf = pdf.head(top)
        return Metric("chute_par_clause", pdf, self._spark)

    # ── 6. Anomalies CPT_ONLY par mois (= graphe 6) — ré-agrège Spark ─────────

    def anomalies_cpt_only(self, date_col: str = "CPT_D_SURVENANCE",
                           pm_col: str = "CPT_PM") -> Metric:
        """Anomalies (CPT sans contrepartie MRM) par mois de survenance.

        Volume et PM compte par mois, avec le marqueur fin d'année
        (Oct-Déc : déclarations tardives probables).
        """
        pdf = (
            self._df
            .filter(F.col("TYPE_RECONCILIATION") == "CPT_ONLY")
            .withColumn("MOIS_SURVENANCE", F.month(F.col(date_col)))
            .withColumn("MOIS_LABEL", _mois_label_expr(date_col))
            .groupBy("MOIS_SURVENANCE", "MOIS_LABEL")
            .agg(
                F.count("*").alias("NB_DOSSIERS"),
                F.round(F.sum(pm_col), 2).alias("PM_CPT"),
            )
            .orderBy("MOIS_SURVENANCE")
            .toPandas()
        )
        if pdf.empty:
            cols = ["MOIS_SURVENANCE", "MOIS_LABEL", "NB_DOSSIERS", "PM_CPT", "IS_FIN_ANNEE"]
            return Metric("anomalies_cpt_only", pd.DataFrame(columns=cols), self._spark)
        pdf["IS_FIN_ANNEE"] = pdf["MOIS_SURVENANCE"].isin([10, 11, 12])
        return Metric("anomalies_cpt_only", pdf, self._spark)

    # ── 7. Conformité globale des consignes (= graphe 8) ──────────────────────

    def conformite_globale(self) -> Metric:
        """Suivi des consignes au global : segments conforme / non retrouvé /
        non conforme (consignes conserver/étudier/ajouter) + suppression effective.

        Une ligne par segment, deux groupes (KAS = conserver/étudier/ajouter,
        DELETE = à supprimer), avec le taux du groupe.
        """
        d, k = self.d, self.k
        cons = d["consignes"]
        nr = cons["À ajouter"]["ko"] + cons["À étudier"]["ko"]     # non retrouvés
        nc = cons["À conserver"]["ko"]                             # non conformes
        c_del  = cons["À supprimer"]
        del_ok = c_del["conf"]
        del_ko = c_del["nb"] - del_ok
        rows = [
            ("conserver/étudier/ajouter", "Conforme",        k["conf"], d["conformite_globale"]),
            ("conserver/étudier/ajouter", "Non retrouvé",    nr,        d["conformite_globale"]),
            ("conserver/étudier/ajouter", "Non conforme",    nc,        d["conformite_globale"]),
            ("à supprimer",               "Supprimé (OK)",   del_ok,    c_del["pct"]),
            ("à supprimer",               "Encore au compte", del_ko,   c_del["pct"]),
        ]
        return self._metric("conformite_globale", [{
            "GROUPE"           : grp,
            "SEGMENT"          : seg,
            "NB_DOSSIERS"      : nb,
            "PCT_CONFORMITE_GROUPE" : pct,
        } for grp, seg, nb, pct in rows])

    # ── Vues filtrées de consignes() — graphes 4 et 9 ─────────────────────────

    def chute_par_consigne(self) -> Metric:
        """Taux de chute par consigne pertinente (= graphe 4)."""
        df = self.consignes().data
        out = df[df["PM_PERTINENTE"]][
            ["CONSIGNE", "TAUX_CHUTE_PCT", "PM_MRM", "PM_CPT", "ECART"]
        ].reset_index(drop=True)
        return Metric("chute_par_consigne", out, self._spark)

    def pm_par_consigne(self) -> Metric:
        """PM revue MRM vs PM compte par consigne pertinente (= graphe 9)."""
        df = self.consignes().data
        out = df[df["PM_PERTINENTE"]][
            ["CONSIGNE", "PM_MRM", "PM_CPT", "ECART", "TAUX_CHUTE_PCT"]
        ].reset_index(drop=True)
        return Metric("pm_par_consigne", out, self._spark)

    def conformite_consignes(self) -> Metric:
        """Conformité par consigne, toutes consignes (= graphe 5)."""
        df = self.consignes().data
        out = df[
            ["CONSIGNE", "NB_TOTAL", "NB_CONFORMES", "PCT_CONFORMITE", "NB_KO", "NATURE_KO"]
        ].copy()
        out["PCT_NON_CONFORME"] = (100 - out["PCT_CONFORMITE"]).round(1)
        return Metric("conformite_consignes", out, self._spark)

    # ── Orchestration ─────────────────────────────────────────────────────────

    def all(self) -> Dict[str, Metric]:
        """Toutes les métriques en un dict {nom: Metric} (réutilisable export)."""
        producers = (
            self.synthese, self.taux_chute_global, self.consignes,
            self.compte_justification, self.couverture_mrm, self.chute_par_clause,
            self.chute_par_consigne, self.conformite_consignes,
            self.anomalies_cpt_only, self.conformite_globale, self.pm_par_consigne,
        )
        return {m.name: m for m in (p() for p in producers)}

    def export(
        self,
        base_path   : str = DEFAULT_BASE_PATH,
        formats     : Iterable[str] = ("csv", "json", "parquet"),
        delta_schema: Optional[str] = None,
    ) -> Dict[str, Metric]:
        """
        Écrit toutes les métriques sur DBFS, sous <base>/<CLIENT>_<PERIM>/metrics.

        formats ⊆ {csv, json, parquet, excel, delta} — excel produit un seul
        .xlsx multi-onglets ; delta requiert delta_schema (une table
        <schema>.itip_metric_<nom>_<perim> par métrique).
        """
        tables  = self.all()
        formats = {f.lower() for f in formats}
        out = _to_local(output_dir(base_path, "metrics"))
        os.makedirs(out, exist_ok=True)
        print(f"[METRICS] périmètre {CLIENT_NAME} / clauses {_PERIMETRE} → {sorted(formats)}")

        for name, metric in tables.items():
            if "csv" in formats:
                print("  ✓ [CSV]     " + metric.to_csv(f"{out}/{name}.csv"))
            if "json" in formats:
                print("  ✓ [JSON]    " + metric.to_json(f"{out}/{name}.json"))
            if "parquet" in formats:
                print("  ✓ [PARQUET] " + metric.to_parquet(f"{out}/{name}.parquet"))
            if "delta" in formats and delta_schema:
                table = f"{delta_schema}.itip_metric_{name}_{_PERIMETRE}"
                (metric.to_spark().write.format("delta").mode("overwrite")
                       .option("overwriteSchema", "true").saveAsTable(table))
                print(f"  ✓ [DELTA]   {table}")

        if "excel" in formats:
            path = f"{out}/metrics_{CLIENT_NAME}_{_PERIMETRE}.xlsx"
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                for name, metric in tables.items():
                    metric.data.to_excel(writer, sheet_name=name[:31], index=False)
            print(f"  ✓ [EXCEL]   {path}")

        print(f"[METRICS] export terminé → {out}\n")
        return tables
