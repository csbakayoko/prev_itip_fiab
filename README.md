# ITIP-FIAB — Fiabilisation de la réconciliation CPT / MRM

Backtesting entre la **revue d'inventaire MRM** (fichier Excel/CSV, à terme lu
depuis SharePoint) et le **compte CPT** (table du Lab Databricks) : contrôles
qualité, matching en cascade, calcul des KPI (chute, couverture, conformité),
puis restitution — 20 tables métriques (Delta / Excel / CSV / Parquet / JSON)
consommées par Power BI, et 11 graphiques.

## Chaîne cible

```
Power BI → Power Automate → Databricks Job
    → lecture Excel SharePoint + tables Lab
    → contrôles qualité → mapping → matching → KPI
    → table Delta + exports (Excel / CSV / JSON)
    → rafraîchissement Power BI
```

## Structure du repo

| Dossier / fichier | Rôle |
|---|---|
| `main.py` | Point d'entrée du pipeline : `build_df_result` (cœur métier) + `run` (avec restitution) |
| `config/` | Toute la configuration — `profile.py` (périmètre, sources, exports), `mappings.py` (colonnes brutes → canoniques), `params.py` (matching, dédoublonnage) |
| `core/io/` | Chargement CPT (Hive/parquet) et MRM (CSV), export Excel Power BI, ponts Excel/SharePoint (`sources.py`, préparé — branché à l'intégration SharePoint) |
| `core/prep/` | Contrôles qualité, nettoyage, dédoublonnage, clés de matching |
| `core/match/` | Waterfall de matching, récupérations (N+1, statut NON), audit de clé |
| `core/synthese/` | Synthèse : passe Spark unique (`compute_synthese`), contrat typé, rendu console |
| `core/metrics/` | Les 20 tables métriques + contrôles de cohérence inter-tables ; `viz.py` = 11 graphiques |
| `notebooks/` | Orchestration uniquement — aucune logique métier |
| `tests/` | Tests unitaires (pytest) — lancés en CI |
| `docs/` | `METRIQUES.md` (contrat formel des métriques) · `GUIDE_KPI.md` (interprétation, exemples chiffrés) |

## Notebooks (Databricks Repos — la racine du repo est sur `sys.path`)

| Notebook | Usage |
|---|---|
| `itip_fiab_powerbi` | **Production** : pipeline → contrôles bloquants → export des 20 tables (Delta + fichiers) |
| `itip_fiab_main` | Run interactif par année d'inventaire (widgets 2023/2024), métriques affichées table par table |
| `itip_fiab_comparaison` | Comparaison côte à côte des inventaires 2023 vs 2024 |
| `itip_fiab_smoke` | Smoke test après `git pull` : tout le pipeline, **aucune écriture** |
| `itip_fiab_key_audit` | Diagnostic : solidité de la clé de matching (read-only) |

## Configuration

Aucun chemin en dur dans `core/` : tout est piloté par `config/profile.py`
(périmètre, vision CPT, date d'inventaire, fichiers MRM, exports) et
`config/params.py` (règles de matching). En production (`DEV_MODE = False`),
le Job Databricks remplit `config.RUN_PARAMS` avant l'import des modules ;
en session interactive, `core.runtime.configurer_run` permet de rejouer
plusieurs inventaires sans redémarrer.

## Développement local

Le runtime cible est Databricks (pyspark/pandas fournis par le DBR — aucune
dépendance épinglée en prod). En local / CI :

```bash
pip install -e ".[dev]"   # pyspark, pandas, openpyxl, pytest, ruff
ruff check .              # lint
pytest                    # tests
```

La CI GitHub Actions (`.github/workflows/ci.yml`) rejoue lint + tests sur
chaque push.

## Git

- `main` : branche de référence livraison (protégée, mise à jour par PR).
- `feat/*`, `fix/*`, `refactor/*` : branches courtes, une intention par branche.
- Une livraison = un tag `vX.Y.Z` (version alignée sur `pyproject.toml`).
