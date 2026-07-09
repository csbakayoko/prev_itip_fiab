# ITIP-FIAB — Fiabilisation de la réconciliation CPT / MRM

Backtesting entre la **revue d'inventaire MRM** (fichier Excel/CSV, à terme lu
depuis SharePoint) et le **compte CPT** (table du Lab Databricks) : contrôles
qualité, matching en cascade, calcul des KPI (chute, couverture, conformité),
puis restitution — 21 tables métriques (Delta / Excel / CSV / Parquet / JSON)
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
| `core/io/` | Chargement CPT (Hive/parquet) et MRM (CSV, Excel, chemins `sharepoint:` via Microsoft Graph), export Excel Power BI, écriture Delta du détail (`save_result.py`) |
| `core/prep/` | Contrôles qualité, nettoyage, dédoublonnage, clés de matching |
| `core/match/` | Waterfall de matching, récupérations (N+1, statut NON), audit de clé |
| `core/synthese/` | Synthèse : passe Spark unique (`compute_synthese`), contrat typé, rendu console |
| `core/metrics/` | Les 21 tables métriques + contrôles de cohérence inter-tables ; `viz.py` = 11 graphiques |
| `notebooks/` | Orchestration uniquement — aucune logique métier |
| `tests/` | Tests unitaires (pytest) — lancés en CI |
| `docs/` | `RECETTE_ETUDE.md` (fabrication de l'étude de bout en bout) · `METRIQUES.md` (contrat formel des métriques) · `GUIDE_KPI.md` (interprétation, exemples chiffrés) · `POWERBI_MAQUETTE.md` (maquette du rapport, branchement SQL Warehouse) |

## Notebooks (Databricks Repos — la racine du repo est sur `sys.path`)

| Notebook | Usage |
|---|---|
| `itip_fiab_powerbi` | **Production** : pipeline → contrôles bloquants → export des 21 tables (Delta + fichiers) |
| `itip_fiab_main` | Run interactif par année d'inventaire (widgets 2023/2024), métriques affichées table par table |
| `itip_fiab_comparaison` | Comparaison côte à côte des inventaires 2023 vs 2024 |
| `itip_fiab_smoke` | Smoke test après `git pull` : tout le pipeline, **aucune écriture** |
| `itip_fiab_key_audit` | Diagnostic : solidité de la clé de matching (read-only) |

## Configuration

Aucun chemin en dur dans `core/` : tout est piloté par `config/profile.py`
(inventaires connus `INVENTAIRES`, périmètre, exports) et `config/params.py`
(règles de matching). Deux surcharges d'environnement pour changer d'espace
sans toucher au code (conf du cluster / du Job) :

- `ITIP_DBFS_HOME` — racine DBFS de travail (sources MRM, checkpoints, exports) ;
- `ITIP_DELTA_SCHEMA` — schéma des tables métriques Delta
  (défaut `hive_metastore.itip_fiab`, `""` = pas d'export Delta).

Un run paramétré (widgets notebook, paramètres de Job) passe par
`core.runtime.configurer_run`, qui surcharge date, vision et fichiers MRM —
et permet aussi de rejouer plusieurs inventaires dans une même session.

**SharePoint (prêt à activer)** : un chemin source `sharepoint:/<chemin dans
la bibliothèque>` déclenche le téléchargement du fichier via Microsoft Graph
avant lecture (`.xlsx` lu nativement). Activation : remplir `SHAREPOINT` dans
`config/profile.py` (app registration Azure AD + secret scope Databricks —
prérequis IT décrits dans `core/io/sources.py`).

## Exploitation — Databricks Job (production)

Le Job lance le notebook **`notebooks/itip_fiab_powerbi`**. Tous les
paramètres sont des widgets, surchargeables en « base parameters » du Job :

| Paramètre | Défaut | Rôle |
|---|---|---|
| `annee_inventaire` | `2023` | sélectionne l'entrée `INVENTAIRES` de la config |
| `date_inventaire` / `vision_cpt` / `fichier_mrm` | vide = config | surcharges unitaires |
| `fichier_mrm_n1` | vide = config | chemin MRM N+1 ; `aucun` = run sans récupération tardive |
| `types_compte` | `PB` | types de compte chargés (`PB,HPB`, … ; `*` = tout le portefeuille) |
| `delta_schema` | `hive_metastore.itip_fiab` | schéma des tables métriques (créé si absent ; vide = pas de Delta) |

Le run est **bloquant** sur les contrôles (lignes toutes classées, taux de
chute cohérent, recoupements inter-tables) : un Job vert = des onglets
Power BI fiables. En sortie : les tables métriques (`<schema>.metrique_*`,
dont `metrique_consignes_par_clause` — le tableau de bord par type de compte
× clause) et le détail dossier par dossier (`<schema>.resultat_backtest`).
Les noms de tables sont **stables** (le périmètre est la colonne `PERIMETRE`,
pas un suffixe de nom) et **toutes les tables Delta sont historisées par run**
(`replaceWhere` sur `DATE_INVENTAIRE × PERIMETRE` : rejouer un run remplace
ses lignes, 2023 et 2024 coexistent) ; colonnes de run `DATE_INVENTAIRE` /
`PERIMETRE` / `LIBELLE_RUN`, lignes ventilées avec `TYPE_COMPTE` (PB/HPB/…)
et `CLAUSE` (nullable). Power BI se connecte au SQL Warehouse, ou importe le
classeur Excel écrit sous `EXPORT_BASE_PATH`.

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
