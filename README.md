# ITIP-FIAB — Fiabilisation de la réconciliation CPT / MRM

Backtesting entre la **revue d'inventaire MRM** (fichier Excel ou CSV déposé
sur DBFS) et le **compte CPT** (table du Lab Databricks) : contrôles qualité,
matching en cascade, calcul des KPI (chute, couverture, conformité), puis
restitution — 8 tables métriques (Delta / Excel / CSV / Parquet / JSON)
consommées par Power BI, et 11 graphiques.

## Chaîne

```
fichier MRM déposé sur DBFS + tables Lab
    → Databricks Job
    → contrôles qualité → mapping → matching → KPI
    → tables Delta du metastore Hive        (sortie de RÉFÉRENCE)
      + fichiers DBFS (Excel / parquet / CSV)  (sortie secondaire)
    → Power BI (SQL Warehouse)
```

Le déclenchement du Job depuis Power BI via Power Automate, et la lecture
directe du fichier depuis SharePoint, restent des évolutions possibles : ni
l'un ni l'autre n'est actif aujourd'hui.

**Périmètre : PB aujourd'hui, élargissement du Lab à venir.** Le compte
CPT/CORECO couvre déjà l'intégralité du portefeuille ; seul le périmètre
**PB** (Participation aux Bénéfices, assurance directe hors réassurance) est
aujourd'hui intégré au **Lab Databricks** sur lequel tourne ce pipeline. Le
prochain basculement porte sur l'**intégration des autres périmètres (HPB,
…) dans le Lab** — pas sur CORECO, déjà en place pour tout le portefeuille.
Le pipeline est déjà prêt : l'axe `TYPE_COMPTE` (PB / HPB / …) est câblé de
bout en bout (voir « Axe d'analyse » plus bas), élargir le périmètre est une
évolution de configuration (`CLIENT_TYPE_CLAUSES`), pas de code.

## Structure du repo

| Dossier / fichier | Rôle |
|---|---|
| `main.py` | Point d'entrée du pipeline : `build_df_result` (cœur métier) + `run` (avec restitution) |
| `config/` | Toute la configuration — `profile.py` (périmètre, sources, exports), `mappings.py` (colonnes brutes → canoniques), `params.py` (matching, dédoublonnage) |
| `core/io/` | Chargement CPT (Hive / parquet) et MRM (CSV ou Excel sur DBFS ; voie SharePoint présente mais désactivée), export Excel Power BI, écriture Delta du détail (`save_result.py`) |
| `core/prep/` | Contrôles qualité, nettoyage, dédoublonnage, clés de matching |
| `core/match/` | Waterfall de matching, récupérations (N+1, statut NON), audit de clé |
| `core/synthese/` | Synthèse : passe Spark unique (`compute_synthese`), contrat typé, rendu console |
| `core/metrics/` | Les 8 tables métriques + contrôles de cohérence inter-tables ; `viz.py` = 11 graphiques |
| `notebooks/` | Orchestration uniquement — aucune logique métier |
| `tests/` | Tests unitaires (pytest) — lancés en CI |
| `docs/` | `RECETTE_ETUDE.md` (fabrication de l'étude de bout en bout) · `METRIQUES.md` (contrat formel des métriques) · `GUIDE_KPI.md` (interprétation, exemples chiffrés) · `POWERBI_MAQUETTE.md` (maquette du rapport, branchement SQL Warehouse) |
| `livrables/` | Documents générés, **dernières versions** (Word / PPT / PDF — seul l'index `README.md` est suivi avec le code) : tutoriels rapport Power BI et Job Databricks, recette de l'étude, cartographie des anomalies, trame d'entretien |

## Notebooks (Databricks Repos — la racine du repo est sur `sys.path`)

| Notebook | Usage |
|---|---|
| `itip_fiab_powerbi` | **Production** : pipeline → contrôles bloquants → export des 8 tables (Delta + fichiers) |
| `itip_fiab_main` | Run interactif par année d'inventaire (widgets 2023/2024), métriques affichées table par table |
| `itip_fiab_comparaison` | Comparaison côte à côte des inventaires 2023 vs 2024 |
| `itip_fiab_smoke` | Smoke test après mise à jour du code : tout le pipeline, **sans export** |
| `itip_fiab_key_audit` | Diagnostic : solidité de la clé de matching (read-only) |
| `itip_fiab_inspect_cpt_parquet` | Diagnostic : écarts de colonnes d'un parquet CPT candidat vs la table Hive (read-only) |

## Configuration

Aucun chemin en dur dans `core/` : tout est piloté par `config/profile.py`
(inventaires connus `INVENTAIRES`, périmètre, exports) et `config/params.py`
(règles de matching). Deux surcharges d'environnement pour changer d'espace
sans toucher au code (conf du cluster / du Job) :

- `ITIP_DBFS_HOME` — racine DBFS de travail (sources MRM, checkpoints, exports) ;
- `ITIP_DELTA_SCHEMA` — schéma des tables métriques Delta
  (défaut `hive_metastore.itip_backtest`, `""` = pas d'export Delta).

Un run paramétré (widgets notebook, paramètres de Job) passe par
`core.runtime.configurer_run`, qui surcharge date, vision et fichiers MRM —
et permet aussi de rejouer plusieurs inventaires dans une même session.

**Qui écrit quoi — le Hive est la sortie de référence.** `EXPORT_ANALYSES = True`
et `EXPORT_FORMATS = ("delta", "excel", "parquet", "csv")` (`config/profile.py`) :
`main.run` — donc le Job comme un `spark-submit main.py` — écrit les 8 tables
métriques **et** le détail `resultat_backtest` dans `EXPORT_DELTA_SCHEMA`
(défaut `hive_metastore.itip_backtest`), puis les fichiers DBFS et les PNG en
sortie **secondaire**.

| Je veux… | Comment |
|---|---|
| Le cœur métier sans aucune écriture | appeler `main.build_df_result` (jamais `run`) — c'est ce que fait `itip_fiab_smoke` |
| Couper le Hive, garder les fichiers | retirer `"delta"` de `EXPORT_FORMATS`, ou `ITIP_DELTA_SCHEMA=""`, ou widget `delta_schema` vide |
| Écrire ailleurs (test) | `ITIP_DELTA_SCHEMA=hive_metastore.itip_backtest_test`, ou le widget du Job |
| Ne rien écrire du tout | `EXPORT_ANALYSES = False` et `EXPORT_GRAPHS = False` |

⚠ **L'écriture Delta remplace la partition du run** (`replaceWhere` sur
`DATE_INVENTAIRE × PERIMETRE`). Rejouer une année remplace exactement ses
lignes — mais un run lancé avec des sources modifiées sous une date officielle
écrase la partition officielle. Pour expérimenter, viser un schéma de test.
L'export Delta **exige** une `DATE_INVENTAIRE` résoluble (`dd/MM/yyyy`) : c'est
la clé d'historisation, une date `"auto"` / `"n/d"` fait échouer le run plutôt
que d'historiser à l'aveugle.

⚠ **Changer les colonnes d'une table = un `DROP TABLE` à passer une fois.**
`replaceWhere` interdit `overwriteSchema` : dès qu'une table Delta existante n'a
plus le même schéma que les données (colonne ajoutée au pipeline, colonne
renommée), le run échoue sur `A schema mismatch detected when writing to the
Delta table`. Les tables sont **dérivées** — le run les recrée :

```sql
DROP TABLE hive_metastore.itip_backtest.resultat_backtest;
```

Seules les partitions d'inventaires que tu ne rejoues pas sont perdues. C'est le
comportement voulu : un échec bruyant vaut mieux qu'une évolution de schéma
silencieuse (`mergeSchema` laisserait cohabiter anciennes et nouvelles colonnes,
à moitié nulles, dans les onglets Power BI).

**Source MRM : dépôt manuel (SharePoint désactivé).** Le fichier d'inventaire
MRM est déposé à la main sur DBFS, puis référencé par son chemin `dbfs:/` dans
`INVENTAIRES` (`config/profile.py`). Le `.csv` comme le `.xlsx` sont acceptés :
le format est déduit de l'extension. Ajouter une année = déposer le fichier,
puis ajouter son entrée dans `INVENTAIRES`.

La voie SharePoint (téléchargement via Microsoft Graph) est **désactivée** —
`SHAREPOINT["actif"] = False`. Le code reste en place : un chemin
`sharepoint:/...` est refusé avec un message explicite au lieu d'être tenté
avec une configuration incomplète. Pour la réactiver quand l'app registration
Azure AD sera disponible, la marche à suivre est en commentaire au-dessus de
`SHAREPOINT` dans `config/profile.py` (prérequis IT détaillés dans
`core/io/sources.py`).

## Exploitation — Databricks Job (production)

Le Job lance le notebook **`notebooks/itip_fiab_powerbi`**. Tous les
paramètres sont des widgets, surchargeables en « base parameters » du Job :

| Paramètre | Défaut | Rôle |
|---|---|---|
| `annee_inventaire` | `2023` | sélectionne l'entrée `INVENTAIRES` de la config |
| `date_inventaire` / `vision_cpt` / `fichier_mrm` | vide = config | surcharges unitaires |
| `fichier_mrm_n1` | vide = config | chemin MRM N+1 ; `aucun` = run sans récupération tardive |
| `types_compte` | `PB` | types de compte chargés (`PB,HPB`, … ; `*` = tout le portefeuille) |
| `delta_schema` | `hive_metastore.itip_backtest` | schéma des tables métriques (créé si absent ; vide = pas de Delta) |

Le run est **bloquant** sur les contrôles (lignes toutes classées, taux de
chute cohérent, recoupements inter-tables) : un Job vert = des onglets
Power BI fiables. En sortie : les tables métriques (`<schema>.metrique_*`,
dont `metrique_consignes_par_type_compte` — le tableau de bord des consignes)
et le détail dossier par dossier (`<schema>.resultat_backtest`).
Les noms de tables sont **stables** (le périmètre est la colonne `PERIMETRE`,
pas un suffixe de nom) et **toutes les tables Delta sont historisées par run**
(`replaceWhere` sur `DATE_INVENTAIRE × PERIMETRE` : rejouer un run remplace
ses lignes, 2023 et 2024 coexistent) ; colonnes de run `DATE_INVENTAIRE` /
`PERIMETRE` / `LIBELLE_RUN`. Power BI se connecte au SQL Warehouse, ou importe
le classeur Excel écrit sous `EXPORT_BASE_PATH`.

**Axe d'analyse : `TYPE_COMPTE`, pas la clause.** Les métriques se ventilent
par type de compte (`PB` / `HPB` / …), le périmètre métier. La clause n'est
**pas** un axe d'analyse : elle sert uniquement à remplacer un RPP nul dans la
clé de matching (côté PB), et tous les types de compte n'en portent pas. Elle
ne subsiste que dans l'angle « Clause (détail) » de `metrique_orphelins`
(détail d'investigation, seuls les dossiers porteurs d'une clause) — pour un
total d'orphelins, lire l'angle « Type de compte » de la même table.
Aujourd'hui `TYPE_COMPTE` ne prend que la valeur `PB` (seul périmètre intégré
au Lab) ; l'élargissement à `HPB` et aux autres périmètres se ventilera sur ce
même axe, sans changement des tables ni du rapport Power BI.

## Développement local

Le runtime cible est Databricks (pyspark/pandas fournis par le DBR — aucune
dépendance épinglée en prod). En local / CI :

```bash
pip install -e ".[dev]"   # pyspark, pandas, openpyxl, pytest, ruff
ruff check .              # lint
pytest                    # tests
```

La CI rejoue lint + tests automatiquement.

Les tests Spark ont besoin d'un JDK (Java) installé localement ; sans lui, ils
échouent sur `JAVA_GATEWAY_EXITED` — les tests de logique pure (pandas)
tournent, eux, sans rien de plus.
