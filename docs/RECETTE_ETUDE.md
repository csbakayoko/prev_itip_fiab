# Recette de l'étude — ITIP-FIAB (fabrication de bout en bout)

> Ce document décrit **comment l'étude est fabriquée**, de la donnée brute à la
> restitution Power BI : les ingrédients (sources), la préparation (nettoyage,
> clés), la cuisson (waterfall de matching + récupérations), le dressage
> (synthèse, 10 tables, 12 graphiques) et les garde-fous (contrôles de
> cohérence). Chaque étape renvoie au module qui la porte.
>
> 📐 **Le contrat formel des métriques** (formules, univers, limites) →
> [`METRIQUES.md`](METRIQUES.md). 📘 **L'interprétation pédagogique des KPI**
> (exemples chiffrés, phrases à l'oral) → [`GUIDE_KPI.md`](GUIDE_KPI.md).

---

## 0. Vue d'ensemble — la recette en un schéma

L'étude réconcilie la **revue d'inventaire MRM** (l'*estimation* : ce qui
devrait être provisionné) avec le **compte CPT / CORECO** (le *réel* : ce qui
est effectivement provisionné). Le pipeline (`main.py`) :

```
CPT (table Lab)          MRM (CSV ou Excel sur DBFS)
      │                          │
      ▼                          ▼
 clean_cpt                  clean_mrm            ← §2 préparation
      │                          │
      │                 scission OUI / NON       ← §3 statut inventaire
      │                     │        │
      └────────┬────────────┘        │ (réservé au repêchage)
               ▼                     │
      matching_waterfall             │           ← §4 cascade (14 étapes)
               │                     │
   récupération N+1 (CPT_LATE)       │           ← §5.1 déclarations tardives
               │                     │
   repêchage statut NON  ◄───────────┘           ← §5.2 (hors métriques)
               │
   obs. tardives IT (CPT_OBS_TARDIVE)            ← §5.3 (hors métriques)
               │
   enrich_result_tags → df_result                ← §6 le résultat
               │
      ┌────────┼──────────────┐
      ▼        ▼              ▼
  synthèse  10 tables      12 graphiques          ← §7 restitution
  console   métriques     (titres-messages)
            (Delta/Excel/CSV/Parquet/JSON)
               │
      contrôles de cohérence (bloquants en prod) ← §8
               │
            Power BI
```

> 📊 **Illustration à placer ici** : la figure « chaîne du pipeline » (schéma
> Excalidraw de la vue d'ensemble, reprise en tête du document HTML) — elle
> remplace avantageusement le schéma ASCII dans les exports Word / PDF.

Deux principes traversent toute la recette :

1. **Une ligne = une catégorie.** Chaque dossier finit avec exactement un
   `TYPE_RECONCILIATION` (vérifié : `classified_rows == total_rows`). Les
   populations « hors métriques » portent un **label distinct** → leur
   exclusion des calculs est garantie **par construction**, pas par filtre.
2. **Du plus strict au plus flexible.** Le matching descend une cascade de
   clés de moins en moins discriminantes ; les récupérations **rejouent la
   même cascade dans le même ordre**. Un assuré à plusieurs sinistres est
   ainsi rapproché de la **bonne** contrepartie avant que les clés lâches ne
   ratissent le reste.

---

## 1. Les ingrédients — sources et périmètre

| Ingrédient | Source | Chargement |
|---|---|---|
| **CPT** (compte) | export parquet prioritaire `CPT_PARQUET_PATH` (`/mnt/lake/compteclient/data/compteclient/tetepartete_itip/prepare/tetepartete_itip.PARQUET`), repli automatique sur la table Hive `compteclient.tetepartete_itip` | `core/io/load_data.py` — filtré sur la **vision comptable** (`CC2023`, …) et les **types de compte** (`PB` par défaut) |
| **MRM courant** (revue) | CSV `;` ou `.xlsx` déposé à la main sur DBFS | `core/io/sources.py` — l'inventaire de référence de l'exercice audité |
| **MRM N+1** (facultatif) | même format, inventaire suivant (ex. 30/06/2024 pour l'exercice 2023) | sert **uniquement** à la récupération des déclarations tardives (§5.1) ; absent → pas de `CPT_LATE` |

Le périmètre est **entièrement piloté par `config/profile.py`** (aucun chemin
en dur dans `core/`) : le dictionnaire `INVENTAIRES` est la **source unique**
des dates / visions / chemins MRM par année ; `CLIENT_TYPE_CLAUSES = ["PB"]`
et `CLIENT_CLAUSES = None` (toutes les clauses) définissent le périmètre courant.
Un run paramétré (widgets notebook, paramètres de Job) passe par
`core.runtime.configurer_run` qui surcharge date, vision et fichiers sans
toucher au code.

> **PB aujourd'hui, pas une limite du compte.** Le compte CPT/CORECO couvre
> déjà l'intégralité du portefeuille ; `CLIENT_TYPE_CLAUSES = ["PB"]` reflète
> le périmètre **actuellement intégré au Lab Databricks**, pas une limite de
> CORECO. Le basculement à venir élargit le Lab aux autres périmètres (HPB,
> …) : côté pipeline, il ne s'agit que d'ajouter une valeur à
> `CLIENT_TYPE_CLAUSES` — l'axe `TYPE_COMPTE` (§6 de `METRIQUES.md`) est déjà
> prêt à ventiler PB / HPB / … sans changement de code.

---

## 2. La préparation — nettoyage et clés (`core/prep/`)

### 2.1 Contrôles qualité d'entrée (non bloquants)

`controle_colonnes` (`core/prep/controls.py`) vérifie, **sur les seules
colonnes que le pipeline consomme** (clés des mappings) : colonnes attendues
absentes de la source, colonnes présentes mais **100 % nulles**. Tout est
tracé en WARNING, le run n'est **jamais interrompu** (choix « WARN +
continue » : en Job automatisé, on débogue a posteriori sans casser la prod).

### 2.2 Pipeline CPT (`clean_cpt`)

1. Contrôle qualité des colonnes brutes ;
2. **Dédoublonnage technique last-write** : par clé métier (`n_rpp`,
   survenance, nom, naissance, date d'arrêt, garantie), on garde la version
   la plus récente (`tech_day` desc) — le tri se fait sur la colonne **brute**,
   avant le mapping ;
3. Sélection / renommage vers les colonnes canoniques (`MAPPING_CPT`,
   `config/mappings.py`). `tech_day` → `TECH_DAY` y figure : la colonne est
   **conservée** dans `df_result`, donc dans `resultat_backtest` — la fraîcheur
   de la source se lit ligne par ligne dans l'export ;
4. Cast des dates (Hive → `date`, `TECH_DAY` inclus) et des montants (`PM`,
   `PSAP` → double, **NULL → 0** : uniformise les exports, neutre pour les
   métriques). Le cast rend le type **déterministe quelle que soit la source**
   (parquet prioritaire ou repli Hive) — sans lui, un repli changerait le type
   d'une colonne et l'écriture Delta échouerait sur un conflit de schéma ;
5. **Imputation garantie IP** : garantie nulle **mais** `D_INVALIDITE`
   renseignée ⇒ `GARANTIE = 64` (IP). Faite **avant** les clés — la clé
   stricte est un `concat_ws` qui ignore les NULL : une garantie nulle
   disparaîtrait de la clé → collision IT/IP d'un même assuré ;
6. Ajout des **clés de matching** (§2.4) puis préfixage `CPT_*`.

### 2.3 Pipeline MRM (`clean_mrm`)

1. Contrôle qualité ; sélection / renommage (`MAPPING_MRM`) ;
2. Cast des dates (CSV français `dd/MM/yyyy`) et montants (virgule décimale
   tolérée, NULL → 0) ;
3. Ajout des clés de matching ;
4. **Dédoublonnage déterministe sur la clé stricte** : à clé égale, on écarte
   **en priorité les statuts inventaire NON** (non remontés à la direction
   financière), puis on garde le plus récent (`D_INVENTAIRE` desc), départage
   stable (PM desc, n° de sinistre) — résultat **reproductible d'un run à
   l'autre**, doublons tracés et justifiés ;
5. Préfixage `MRM_*`.

### 2.4 Les clés de matching (`add_matching_keys`)

Neuf clés composites, posées **des deux côtés**, de la plus stricte à la plus
flexible. Composants : RPP (CPT) / IDCORP (MRM), date de naissance,
survenance au jour, code garantie, nom normalisé (upper, sans espaces).

| Clé | Survenance | Identité | Discriminant | Étape servie |
|---|---|---|---|---|
| `key_strict` | jour exact | nom complet | RPP | MATCH_EXACT |
| `key_no_date` | — | nom complet | RPP | MATCH_WINDOW, MATCH_RECHUTE |
| `key_strict_tronc` | jour exact | nom **tronqué 20 car.** | RPP | MATCH_TRONC |
| `key_no_date_tronc` | — | nom tronqué 20 car. | RPP | MATCH_TRONC_WINDOW, MATCH_RECHUTE_TRONC |
| `key_no_garantie` | jour exact | nom complet | RPP (sans garantie) | MATCH_IP |
| `key_clause_strict` | jour exact | nom complet | **clause** (à la place du RPP) | MATCH_CLAUSE |
| `key_clause_no_date` | — | nom complet | clause | MATCH_CLAUSE_WINDOW |
| `key_clause_strict_tronc` | jour exact | nom tronqué | clause | MATCH_CLAUSE_TRONC |
| `key_clause_no_date_tronc` | — | nom tronqué | clause | MATCH_CLAUSE_TRONC_WINDOW |

Deux subtilités métier :

- **Troncature 20 caractères** : le système CPT tronque `NOM_PRENOM` à 20
  caractères à la saisie ; MRM stocke le nom complet. Les clés `_tronc`
  appliquent `LEFT(20)` **des deux côtés** pour rattraper ces dossiers
  (ex. « REICHENAUER CHRISTELLE » MRM vs « REICHENAUER CHRISTEL » CPT).
- **Clés clause = clés de secours** : quand le RPP compte est nul / mal
  renseigné, toutes les clés RPP échouent et le dossier finirait `CPT_ONLY`
  malgré une vraie contrepartie. La clause normalisée (préfixe type retiré :
  `CPB_121981` → `121981`) remplace le RPP. **Garde anti-collision** : la clé
  vaut NULL si la clause manque (sinon `concat_ws` l'ignorerait et la clé
  matcherait entre clauses différentes). La clause étant moins discriminante
  (partagée par tout un contrat), ces clés passent **en dernier** et ne
  déclinent **pas** les étapes IP / rechute.

---

## 3. La scission statut inventaire (OUI / NON)

Avant le matching, le MRM clean est scindé (`_split_mrm_statut`, `main.py`) :

- **OUI + statut absent** → alimente le **matching principal** ;
- **NON** → PM MRM toujours = 0 (non remontée à la direction financière) :
  réservé à la **passe de repêchage** des orphelins compte (§5.2), jamais au
  matching. Un NON qui ne repêche rien n'est **jamais unionné** → zéro
  empreinte dans la volumétrie (pas de `MRM_MISSING` en statut NON).

Conséquence structurelle : `MRM_MISSING` ne contient que des OUI, et les
métriques de valeur ne sont jamais polluées par des couples à PM MRM = 0.

---

## 4. La cuisson — le waterfall de matching (`core/match/waterfall.py`)

À chaque étape : join sur la clé courante (MRM **dédoublonné sur la clé**
avant le join → un CPT matche au plus 1 MRM, pas de démultiplication), filtre métier
optionnel, puis **anti-join** — les lignes matchées sortent, les restantes
descendent à l'étape suivante. Chaque étape est matérialisée (checkpoint
DBFS) : la lignée Spark reste plate, le run survit à l'autoscaling.

Les 14 étapes, **dans l’ordre** :

| # | Étape | `TYPE_RECONCILIATION` | Clé | Condition supplémentaire |
|---|---|---|---|---|
| 1 | Nominale exacte | `MATCH_EXACT` | `key_strict` | — |
| 2 | Fenêtre ±14 j | `MATCH_WINDOW` | `key_no_date` | \|Δ survenance\| ≤ 14 j |
| 3 | Tronquée exacte | `MATCH_TRONC` | `key_strict_tronc` | — |
| 4 | Tronquée + fenêtre | `MATCH_TRONC_WINDOW` | `key_no_date_tronc` | \|Δ survenance\| ≤ 14 j |
| 5 | Passage IT → IP | `MATCH_IP` | `key_no_garantie` | \|garantie CPT − MRM\| == 4 (60 → 64) |
| 6 | Rechute IT | `MATCH_RECHUTE` | `key_no_date` | même garantie, 0 < \|Δ\| ≤ 30 j |
| 7 | Rechute (tronquée) | `MATCH_RECHUTE_TRONC` | `key_no_date_tronc` | idem |
| 8–11 | Clé clause (secours) | `MATCH_CLAUSE[_WINDOW/_TRONC/_TRONC_WINDOW]` | `key_clause_*` | mêmes variantes que 1–4 |
| 12 | **Suppression conforme** | `MRM_DELETE` | résiduel MRM « à supprimer » | consigne « PM MRM à supprimer » (`categorize_mrm_conclusion`) |
| 13 | Orphelins compte | `CPT_ONLY` | résiduel CPT | — |
| 14 | Orphelins revue | `MRM_MISSING` | résiduel MRM | — |

Points de recette :

- **Tout le MRM traverse TOUTES les étapes de matching**, « à supprimer »
  compris. `MRM_DELETE` est un **état terminal**, étiqueté au même niveau que
  les orphelins (étape 12) : le dossier n'a été retrouvé par **aucune** clé,
  donc il a bien disparu du compte — la consigne est suivie. Une « à
  supprimer » retrouvée reste un **match** (`ENCORE_AU_COMPTE`, suppression non
  suivie).
- **Pourquoi à la fin.** Le verdict DELETE se juge sur la présence au compte :
  il n'est comparable aux autres consignes que si le dossier a été cherché avec
  la **même cascade**. Écarter les DELETE plus tôt les privait des clés de
  secours (IP, rechute, clause) et produisait deux erreurs : un dossier toujours
  au compte mais atteignable seulement par une clé lâche passait pour
  « supprimé » (faux conforme), et sa ligne CPT, privée de contrepartie,
  remontait en `CPT_ONLY` — une **fausse anomalie** envoyée à l'investigation.
- L'étape IP valide le rapprochement par l'**offset de garantie** exactement
  égal à 4 (60 = IT → 64 = IP) : sinon faux positif, le dossier reste orphelin.
- Regroupement pour la restitution (`config/params.py`) : **principale**
  (1–2), **affinée** (3–4), **récupération** (5–7), **clé clause** (8–11) —
  tous sont des matchés légitimes (`MATCH_LABELS`), la clé clause est suivie
  à part pour auditer cette clé moins stricte.

> 📊 **Illustration à placer ici** : la figure « cascade de matching »
> (entonnoir des 14 étapes, du plus strict au plus flexible, avec les
> volumétries par étape) — c'est le visuel qui fait comprendre la méthode en
> une image dans les restitutions.

---

## 5. Les secondes chances — récupérations post-waterfall (`core/match/recovery.py`)

Trois passes successives donnent une seconde chance aux `CPT_ONLY`, chacune
avec un label dédié qui fixe son sort dans les métriques :

### 5.1 Déclarations tardives N+1 → `CPT_LATE` (inclus, analyse séparée)

Les orphelins compte sont rejoués contre le **MRM N+1** (OUI + statut absent
seulement) via `RECOVERY_KEYS` : **le waterfall principal rejoué dans le même
ordre et avec les mêmes règles** (EXACT → WINDOW → TRONC → TRONC_WINDOW → IP
→ RECHUTE → RECHUTE_TRONC → clause) — aucune étape sans contrainte de date.
L'étape gagnante est tracée dans `LATE_KEY`. Un `CPT_LATE` a une vraie
contrepartie MRM (d'un autre inventaire) : il compte dans la justification du
compte, mais sa chute et ses consignes sont une **analyse séparée, hors stats
globales** (cf. [`METRIQUES.md`](METRIQUES.md) §4.2).

### 5.2 Repêchage statut NON → `CPT_RECUP_NON` (exclu de toutes les métriques)

Les `CPT_ONLY` restants sont rejoués contre les **MRM NON des deux
exercices** (N et N+1, `LATE_SOURCE = STATUT_NON / STATUT_NON_N1`), même
cascade. Un dossier repêché prouve qu'une contrepartie existe — mais PM MRM
= 0 ⇒ label distinct ⇒ **exclu par construction de toutes les métriques**,
présenté dans l'analyse dédiée `recup_statut_non`. Auto-contrôle : l'hypothèse
« NON ⇒ PM MRM = 0 » est vérifiée à chaque run (`recup_non_pm_mrm_ok`).

### 5.3 Observations tardives IT → `CPT_OBS_TARDIVE` (exclu, explicable)

Les `CPT_ONLY` **garantie 60 (IT)** survenus en **fin d'année** (mois 11–12),
absents du MRM courant **et** du N+1 : le sinistre s'est vraisemblablement
clos avant l'inventaire suivant — il est **logique** de ne pas le retrouver.
**Ce ne sont pas des anomalies** : tagués à part, exclus des taux et des PM,
présentés en volumétrie + PM compte.

### 5.4 Tags persistants (`enrich_result_tags`)

Dernière touche avant de servir : `MRM_ACTION` (consigne reformatée),
`TAG_CPT_ONLY` (segmentation des orphelins définitifs : fin d'année /
PM > 20 000 € / à analyser) et les **dimensions d'export par ligne** —
`CLAUSE` (nullable : un compte sans n° de clause est une donnée légitime),
`TYPE_COMPTE` (PB / HPB / préfixe brut si non mappé — jamais null muet),
`REMONTE_DF` (statut inventaire direction financière).

---

## 6. Le plat — `df_result`

Une ligne par dossier, une catégorie par ligne (`TYPE_RECONCILIATION`) :

| Famille | Labels | Sort dans les métriques |
|---|---|---|
| Matchés inventaire courant | `MATCH_EXACT/_WINDOW/_TRONC/_TRONC_WINDOW/_IP/_RECHUTE/_RECHUTE_TRONC/_CLAUSE*` | **base des stats globales** (hors « à supprimer » / statut NON) |
| Récupérés N+1 | `CPT_LATE` | inclus, **analyse séparée** (chute N+1, suivi N+1) |
| Repêchés statut NON | `CPT_RECUP_NON` | **exclu de tout**, analyse dédiée |
| Obs. tardives IT | `CPT_OBS_TARDIVE` | **exclu de tout**, présenté (explicable) |
| Orphelins revue | `MRM_MISSING` | couverture MRM, conformité (non retrouvé) |
| Suppression conforme | `MRM_DELETE` | conformité DELETE, taux de suppression effective |
| Anomalies | `CPT_ONLY` | couverture compte, analyses d'orphelins |

Le détail dossier par dossier est persisté en Delta
(`<schema>.resultat_backtest`, historisé par date d'inventaire) pour les
analyses fines Power BI.

---

## 7. Le dressage — restitution

La restitution se fait en trois étages, **à partir d'une passe Spark unique**
(`compute_synthese`, `core/synthese/`) dont le dict est réutilisé partout —
aucune grandeur n'est recalculée deux fois, donc aucune ne peut diverger :

1. **Synthèse console** (`print_synthese`) : vue d'ensemble ASCII — bulles
   REVUE / RETROUVÉS / COMPTE, taux, niveaux de PM, blocs séparés N+1 et
   statut NON — dans le **vocabulaire client à deux couches** (*retrouvé /
   non retrouvé* = le fait ; *conforme / encore au compte* = le verdict, cf.
   [`METRIQUES.md`](METRIQUES.md) §0).
2. **10 tables métriques** (`core/metrics/`, contrat complet →
   [`METRIQUES.md`](METRIQUES.md) §6) : `dim_run` (la dimension de run —
   pivot du modèle en étoile, reliée à toutes les tables par la clé de
   liaison `CLE_RUN`), `synthese` (tous les KPI en 1 ligne / run,
   historisable), `bilan_cas`, `couverture`, `chute` (dont l'axe « Tranche
   d'écart » : la distribution des écarts de PM par seuils — la volumétrie
   des dossiers sur/sous-provisionnés), `consignes`,
   `consignes_par_type_compte` (le tableau de bord TYPE_COMPTE × CONSIGNE),
   `orphelins`, `controles_coherence` — une table = un sujet complet, les
   angles d'analyse (exercice, type de compte, ancienneté, tranche d'écart,
   garantie, univers…)
   sont des **colonnes** (`EXERCICE`, `AXE`, `SEGMENT`, `UNIVERS`), jamais des
   tables séparées. Écrites **en Delta dans le metastore
   Hive** — la sortie de **référence**, celle que Power BI interroge : chaque
   table est **historisée par `DATE_INVENTAIRE × PERIMETRE`** (rejouer un
   inventaire remplace exactement ses lignes, 2023 et 2024 coexistent). Les
   fichiers **Excel / parquet / CSV** sur DBFS sont la sortie **secondaire**
   (import Power BI sans Warehouse, dépannage, partage). Le détail dossier par
   dossier part dans `resultat_backtest`, même historisation, même `CLE_RUN`.
3. **12 graphiques-messages** (`core/metrics/viz.py`) : le titre porte la
   conclusion (justification du compte, couverture de la revue, chute par
   type de compte / consigne / ancienneté, distribution des écarts,
   conformité, anomalies, orphelins). Affichés en notebook + PNG sur DBFS.

> 📊 **Emplacement des graphiques dans les documents de restitution** — les
> PNG (`1_… .png` à `12_… .png`, dossier `graphiques` des exports DBFS) ont
> chacun UNE place naturelle : 1-2 (couverture) illustrent la partie
> « justification / couverture », 7 puis 3-10-12 la partie
> « provisionnement » (le KPI d'abord, les ventilations ensuite, la
> distribution en zoom), 4-9 le par-consigne, 5-8 la conformité, 6-11
> l'investigation des orphelins. Même ordre dans le rapport Power BI
> ([`POWERBI_MAQUETTE.md`](POWERBI_MAQUETTE.md)), le deck de restitution et
> les documents : l'étude raconte partout la même histoire dans le même
> ordre.

> **Qui écrit ?** `main.run` (donc le Job) — piloté par `EXPORT_ANALYSES`,
> `EXPORT_FORMATS`, `EXPORT_DELTA_SCHEMA` de `config/profile.py`.
> `main.build_df_result` n'écrit **jamais** rien : c'est le cœur métier seul,
> ce qu'utilise le smoke test.

---

## 8. Les garde-fous — contrôles de cohérence à chaque étage

| Contrôle | Où | Vérifie | Effet |
|---|---|---|---|
| `controle_colonnes` | chargement | colonnes attendues absentes / 100 % nulles | WARNING, non bloquant |
| `classified_rows == total_rows` | `compute_synthese` | toute ligne tombe dans exactement une catégorie connue | signale un `TYPE_RECONCILIATION` inattendu |
| `chute_coherente` | synthèse | chute globale == Σ consignes KAS + sans consigne (même univers) | WARNING sinon |
| `recup_non_pm_mrm_ok` | synthèse | hypothèse « statut NON ⇒ PM MRM = 0 » sur les repêchés | WARNING sinon (l'exclusion ne serait plus neutre) |
| `controles_coherence` | export métriques | recoupements inter-tables : Σ blocs par type de compte / ancienneté == base chute, Σ ventilations d'orphelins == `CPT_ONLY`, totaux de `bilan_cas`… | table exportée ; **assert bloquant** dans le run de production |

Le run de production (`notebooks/itip_fiab_powerbi.py`) est **bloquant** sur
ces contrôles : un Job vert = des onglets Power BI qui racontent **une seule
histoire** (une même grandeur a la même valeur dans tous les onglets).

---

## 9. Exécuter la recette — orchestration

Les notebooks ne portent **aucune logique métier** (orchestration seule) ;
le cœur est `build_df_result` / `run` dans `main.py`.

| Contexte | Entrée | Particularité |
|---|---|---|
| **Production** (Databricks Job) | `notebooks/itip_fiab_powerbi` | widgets / base parameters (`annee_inventaire`, `fichier_mrm_n1`, `types_compte`, `delta_schema`…) ; contrôles **bloquants** ; export des 10 tables + `resultat_backtest` — mise en place pas à pas : [`TUTORIEL_JOB_DATABRICKS.md`](TUTORIEL_JOB_DATABRICKS.md) |
| **Recette vision CC2023** | `notebooks/itip_fiab_vision_cc2023` | l'exercice 2023 déroulé et commenté de bout en bout (tables + distribution des écarts + graphiques + **clauses PB à investiguer**, §4.7), **aucune écriture** |
| **Recette vision CC2024** | `notebooks/itip_fiab_vision_cc2024` | idem pour 2024 — sans MRM N+1 (blocs N+1 vides, récupération « non mesurée »), **aucune écriture** |
| Investigation clause | `notebooks/itip_fiab_exploration_clauses` | une clause en widget → lignes brutes du compte, colonnes de traçabilité (qui a réalisé le compte), balayage des tables du schéma `compteclient`, requêtes libres (read-only) |
| Run interactif | `notebooks/itip_fiab_main` | widgets par année (2023 / 2024), métriques table par table ; **la cellule d'export écrit dans le metastore** (viser un schéma de test pour expérimenter) |
| Comparaison | `notebooks/itip_fiab_comparaison` | 2023 vs 2024 côte à côte (via `configurer_run`, plusieurs inventaires dans une session) |
| Smoke test | `notebooks/itip_fiab_smoke` | tout le pipeline après mise à jour du code via `build_df_result`, **aucune écriture** (ni Delta, ni fichier, ni PNG) |
| Diagnostic clé | `notebooks/itip_fiab_key_audit` | solidité de la clé de matching (read-only) |

Surcharges d'environnement (conf cluster / Job, sans toucher au code) :
`ITIP_DBFS_HOME` (racine de travail) et `ITIP_DELTA_SCHEMA` (schéma Delta,
`""` = pas d'export). En local / CI : `pip install -e ".[dev]"`, `ruff
check .`, `pytest` (rejoués automatiquement par l'intégration continue
à chaque mise à jour du code).

---

## 10. Les dosages — paramètres de la recette (`config/params.py`)

| Paramètre | Valeur | Rôle |
|---|---:|---|
| `WINDOW_DAYS` | 14 | tolérance ± jours des étapes « window » (écart de survenance admis) |
| Troncature nom | 20 car. | limite technique CPT sur `NOM_PRENOM` (clés `_tronc`) |
| `CODE_GARANTIE_IT` / `IP` | 60 / 64 | codes garantie de référence — tout le reste en dérive |
| `IP_GARANTIE_OFFSET` | 4 | validation du passage IT → IP (= 64 − 60) ; `None` désactive l'étape |
| `RELAPSE_WINDOW_DAYS` | 30 | fenêtre max pour rattacher une rechute IT |
| `ORPHAN_FIN_ANNEE_MOIS` | (11, 12) | mois « fin d'année » (obs. tardives IT, segmentation orphelins) |
| `ORPHAN_PM_THRESHOLD` | 20 000 € | seuil « orphelin montant élevé » (`TAG_CPT_ONLY`) |
| `SEUILS_ECART_PM` | ±1 k€ / ±5 k€ / ±20 k€ / ±100 k€ | seuils de la distribution des écarts de PM (axe « Tranche d'écart » de `chute`, graphe 12) — tranches symétriques, libellés et bornes dérivés |

Chaque dosage vit à **un seul endroit** : changer la fenêtre, l'offset IP ou
le seuil orphelin ne demande qu'une ligne de config — la cascade, les
récupérations et les tags en dérivent.
