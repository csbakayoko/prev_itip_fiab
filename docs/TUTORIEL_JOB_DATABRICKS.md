# Tutoriel — construire et lancer le Job Databricks « Backtest ITIP »

> **Objet** : mettre en place, lancer, planifier et dépanner le Job Databricks
> qui exécute le notebook de production `notebooks/itip_fiab_powerbi` et
> alimente les tables consommées par le rapport Power BI.
>
> **Résultat attendu d'un run vert** : les 10 tables métriques
> (`hive_metastore.itip_backtest.metrique_*`, dont la dimension
> `metrique_dim_run`) et le détail `resultat_backtest`, historisés par
> `DATE_INVENTAIRE × PERIMETRE`, tous reliés par la clé `CLE_RUN` — plus le
> classeur Excel et les fichiers CSV / Parquet sur DBFS en sortie secondaire.
>
> 📚 En complément : [`RECETTE_ETUDE.md`](RECETTE_ETUDE.md) (ce que fait le
> pipeline), [`METRIQUES.md`](METRIQUES.md) (le contrat des tables),
> [`POWERBI_MAQUETTE.md`](POWERBI_MAQUETTE.md) (le branchement du rapport).

---

## 1. Vue d'ensemble — ce que fait le Job

```
Job Databricks « ITIP-FIAB Backtest »
   └─ tâche unique : notebook notebooks/itip_fiab_powerbi
        1. lit les paramètres (widgets ← « base parameters » du Job)
        2. pipeline complet (chargement → matching → récupérations → tags)
        3. contrôles de cohérence BLOQUANTS (avant toute écriture)
        4. écrit les 10 tables métriques (Delta, référence + fichiers DBFS)
        5. écrit le détail resultat_backtest
        6. récapitule les tables à brancher dans Power BI
```

Deux principes à garder en tête :

- **Un Job vert = des tables cohérentes.** Les contrôles (lignes toutes
  classées, taux de chute cohérent, recoupements inter-tables) arrêtent le
  run **avant** d'écrire quoi que ce soit d'incohérent. Ne jamais rafraîchir
  Power BI sur un run rouge.
- **Rejouer = remplacer proprement.** L'écriture est historisée par
  `DATE_INVENTAIRE × PERIMETRE` : relancer un inventaire remplace exactement
  ses lignes ; 2023 et 2024 coexistent dans les mêmes tables.

---

## 2. Prérequis (une fois)

| # | Prérequis | Comment vérifier |
|---|---|---|
| 1 | **Accès à l'espace de travail Databricks** avec droit de créer des Jobs et un cluster (ou d'utiliser un cluster partagé existant) | menu « Workflows » visible |
| 2 | **Le dossier Repos du projet** présent dans l'espace de travail et synchronisé avec la dernière version du dépôt de code | Repos ▸ `prev_itip_fiab` ▸ bouton de synchronisation — les notebooks apparaissent sous `notebooks/` |
| 3 | **Le fichier MRM de l'inventaire** déposé sur DBFS au chemin attendu par `config/profile.py` (`INVENTAIRES["<année>"]["mrm"]`, ex. `.../MRM_FILES/MRM_Fiab_31_12_23_V3.csv`) — `.csv` et `.xlsx` acceptés | `dbutils.fs.ls("dbfs:/FileStore/shared_uploads/<compte>/MRM_FILES/")` |
| 4 | *(Recommandé)* **Le fichier MRM N+1** (inventaire suivant, ex. 30/06) pour activer la récupération des déclarations tardives | idem — sinon lancer avec `fichier_mrm_n1 = aucun` |
| 5 | **La table compte accessible** : export parquet `CPT_PARQUET_PATH` (prioritaire) ou table Hive `compteclient.tetepartete_itip` (repli automatique) | `spark.read.parquet(...)` ou `SHOW TABLES IN compteclient` |
| 6 | **Un cluster** avec un Databricks Runtime récent (pyspark, pandas, matplotlib et openpyxl sont fournis par le runtime — rien à installer) | démarrer le cluster, attacher un notebook, exécuter la 1re cellule |

> 💡 **Environnement de travail différent ?** Deux variables d'environnement
> à poser dans la configuration du cluster (Advanced options ▸ Spark ▸
> Environment variables), sans toucher au code :
> `ITIP_DBFS_HOME` (racine DBFS des sources / exports) et
> `ITIP_DELTA_SCHEMA` (schéma des tables ; vide = pas d'écriture Delta).

---

## 3. Avant le premier Job : valider le pipeline (5 min)

1. Ouvrir `notebooks/itip_fiab_smoke` sur le cluster.
2. « Run all » : le smoke test déroule **tout** le pipeline, les 10 tables et
   les 12 graphiques **sans rien écrire**.
3. Toutes les cellules vertes → le code et les données sont prêts, passer à
   la création du Job. Une cellule rouge → traiter d'abord (cf. §8, les
   messages sont explicites).

Pour une recette détaillée d'une vision (tables commentées une à une) :
`notebooks/itip_fiab_vision_cc2023` ou `itip_fiab_vision_cc2024`.

---

## 4. Créer le Job (pas à pas)

Dans le menu latéral : **Workflows ▸ Jobs ▸ « Create job »**.

### 4.1 La tâche (task)

| Champ | Valeur |
|---|---|
| Task name | `backtest_itip_powerbi` |
| Type | **Notebook** |
| Source | **Workspace** (le dossier Repos du projet) |
| Path | `<dossier Repos>/prev_itip_fiab/notebooks/itip_fiab_powerbi` |
| Cluster | un « Job cluster » dédié (recommandé : créé au lancement, arrêté à la fin — pas de coût à vide) ou un cluster partagé existant |

> ⚙️ **Dimensionnement indicatif du Job cluster** : un pilote + 2 à 4
> exécutants de gamme standard suffisent largement (volumétrie de l'ordre de
> quelques dizaines de milliers de lignes par source). L'autoscaling est
> supporté : le pipeline matérialise ses étapes par points de reprise fiables
> sur DBFS (`CHECKPOINT_DIR`), il survit à la réduction du cluster.

### 4.2 Les paramètres (« base parameters »)

Chaque paramètre du Job correspond à un **widget** du notebook : le Job peut
surcharger n'importe lequel, un paramètre vide = le défaut de
`config/profile.py` (dictionnaire `INVENTAIRES`).

| Paramètre | Exemple | Rôle |
|---|---|---|
| `annee_inventaire` | `2023` | sélectionne l'inventaire (entrée du dictionnaire `INVENTAIRES`) |
| `date_inventaire` | *(vide)* ou `31/12/2023` | date du run `jj/mm/aaaa` — pilote l'historisation et l'ancienneté |
| `vision_cpt` | *(vide)* ou `CC2023` | vision comptable du compte |
| `fichier_mrm` | *(vide)* ou `dbfs:/...csv` | chemin du MRM courant |
| `fichier_mrm_n1` | *(vide)*, `dbfs:/...csv` ou `aucun` | MRM N+1 ; `aucun` = run **sans** récupération tardive |
| `types_compte` | `PB` (défaut) ; `PB,HPB` ; `*` | périmètre chargé (types de compte) |
| `delta_schema` | `hive_metastore.itip_backtest` | schéma cible des tables ; **vide = pas d'écriture Delta** (fichiers seulement) |
| `reinitialiser_tables` | `non` (défaut) ; `oui` | **migration en un clic** : `oui` purge les tables du schéma cible (`metrique_*`, anciennes `itip_metric_*`, `resultat_backtest`) avant l'export, qui les recrée proprement — à utiliser une fois après un changement de schéma des tables, puis repasser à `non` |

Configuration minimale pour le run officiel 2023 : `annee_inventaire = 2023`
et tout le reste vide (les défauts font foi).

### 4.3 Fiabilité du Job (recommandé)

- **Retries** : 0 — un échec est un signal métier (contrôle bloquant), pas
  un aléa technique à rejouer en boucle.
- **Notifications** : courriel de l'équipe sur échec (et sur succès pendant
  la période de rodage).
- **Timeout** : 2 h (large — un run nominal se compte en dizaines de
  minutes ; un dépassement signale un problème de données ou de cluster).
- **Exécutions simultanées** (« Maximum concurrent runs ») : **1** — deux
  runs simultanés sur le même inventaire écriraient la même partition.

---

## 5. Lancer et lire un run

1. **« Run now »** (ou « Run now with different parameters » pour surcharger
   ponctuellement, ex. `annee_inventaire = 2024`).
2. Suivre l'exécution : la sortie du notebook s'affiche cellule par cellule.
   Les jalons d'un run sain :
   - `⚙️ Run inventaire 2023 : {...}` — les paramètres appliqués ;
   - `🏗️ df_result : NNN NNN lignes` — le pipeline a produit son résultat ;
   - `✅ Contrôles de synthèse OK — export autorisé.` ;
   - `[METRICS] ✔ contrôles inter-tables : NN/NN OK` puis la liste
     `✓ [DELTA] hive_metastore.itip_backtest.metrique_...` ;
   - `🗄️ Détail du run → ...resultat_backtest` ;
   - le récapitulatif final vert (tables + modèle en étoile `CLE_RUN`).
3. **Après le run** : rafraîchir le jeu de données Power BI (manuel ou
   planifié après le Job) — jamais sur un run rouge.

### Lancer la vision 2024

`Run now with different parameters` ▸ `annee_inventaire = 2024`. Vérifier
d'abord que le fichier `MRM_Fiab_31_12_24` est déposé (prérequis 3) ; tant
que l'inventaire du 30/06/2025 n'existe pas, laisser `fichier_mrm_n1` vide
(la config 2024 n'en définit pas) — les blocs « Récupérés N+1 » resteront
vides, c'est attendu (la colonne `AVEC_MRM_N1` de `metrique_dim_run` le
documente pour le rapport).

---

## 6. Planifier (production semestrielle)

Le rythme naturel du backtest est **semestriel** (à chaque arrêté /
inventaire). Dans l'onglet « Schedules & Triggers » du Job :

- **Planification** : ex. `0 0 7 5 1,7 ?` (le 1er lundi de janvier et de
  juillet à 07 h) — à caler sur la disponibilité du fichier MRM ;
- ou **déclenchement manuel** au dépôt du fichier (mode actuel : le fichier
  MRM est déposé à la main, lancer le Job juste après) ;
- enchaîner le **rafraîchissement Power BI** : planification du jeu de
  données après l'heure du Job, ou orchestration externe (l'appel du Job par
  Power Automate est une évolution possible, non active aujourd'hui).

> 📌 Une **nouvelle année d'inventaire** = déposer le fichier MRM sur DBFS,
> ajouter son entrée dans `INVENTAIRES` (`config/profile.py`), synchroniser
> le dossier Repos, puis lancer avec `annee_inventaire = <année>`. Aucun
> autre changement : noms de tables et rapport inchangés.

---

## 7. Où vont les sorties

| Sortie | Emplacement | Usage |
|---|---|---|
| **Tables métriques** (référence) | `hive_metastore.itip_backtest.metrique_*` (10 tables) | Power BI via SQL Warehouse — historisées par run |
| Détail dossier par dossier | `hive_metastore.itip_backtest.resultat_backtest` | analyses fines, drillthrough |
| Classeur Excel | `dbfs:/.../itip_fiab_exports/<CLIENT>_<PERIM>/metrics/metrics_*.xlsx` | import Power BI sans Warehouse, partage |
| CSV / Parquet | même dossier `metrics/` | rejeu, autres outils |
| Graphiques PNG | `.../graphiques/1_….png` à `12_….png` | documents et supports |

---

## 8. Dépannage — erreurs connues

| Symptôme | Cause | Remède |
|---|---|---|
| `❌ Lignes non classées : {...} — export interrompu` | un `TYPE_RECONCILIATION` inattendu (évolution de code incomplète) | corriger le code / resynchroniser le dossier Repos — rien n'a été écrit |
| `❌ Taux de chute ≠ Σ consignes + hors consigne` | incohérence d'univers entre le taux principal et le par-consigne | anomalie de code à investiguer (voir les logs de synthèse) — rien n'a été écrit |
| `✘ N contrôle(s) inter-tables KO` + assert final | deux tables ne racontent pas la même histoire | lire la table `controles_coherence` affichée au-dessus (colonnes ATTENDU / OBTENU) |
| `A schema mismatch detected...` ou `Écriture Delta refusée pour <table>...` | le schéma d'une table a évolué (colonne ajoutée / renommée — ex. ajout de `CLE_RUN`, `ORDRE`, sortie de la distribution des écarts) et l'écriture historisée refuse de le faire évoluer en silence | relancer une fois avec `reinitialiser_tables = oui` (purge + recréation automatiques), ou passer manuellement `DROP TABLE <schema>.<table>` — seules les partitions non rejouées sont perdues |
| `date_inventaire invalide (...) — impossible d'historiser le run` | date non résoluble (`auto`, `n/d`, faute de frappe) alors que l'export Delta exige une vraie date | renseigner `date_inventaire` au format `jj/mm/aaaa` (ou corriger la config) |
| `Path does not exist` / lecture MRM en échec | fichier MRM absent du chemin `INVENTAIRES` / widget | déposer le fichier sur DBFS ou corriger le chemin (prérequis 3) |
| Le run écrit dans le mauvais schéma | widget `delta_schema` surchargé (ex. schéma de test oublié) | vérifier les « base parameters » du Job ; vide = défaut `hive_metastore.itip_backtest` |
| Perte d'un exécutant en cours de run (autoscaling) | comportement normal | rien à faire : les points de reprise fiables sur DBFS (`CHECKPOINT_DIR`) protègent le run |

> 🧪 **Pour expérimenter sans risque** : lancer avec
> `delta_schema = hive_metastore.itip_backtest_test` (les tables officielles
> ne sont pas touchées), ou `delta_schema` vide (aucune écriture Delta,
> fichiers seulement), ou utiliser les notebooks de recette (aucune
> écriture du tout).

---

## 9. Récapitulatif — la liste de contrôle

- [ ] Dossier Repos synchronisé avec la dernière version du code
- [ ] Fichier(s) MRM déposé(s) sur DBFS, chemins conformes à `INVENTAIRES`
- [ ] Smoke test vert (`itip_fiab_smoke`)
- [ ] Job créé : tâche notebook `itip_fiab_powerbi`, Job cluster, 1 run
      simultané max, notifications d'échec
- [ ] `annee_inventaire` renseigné, autres paramètres vides (défauts) sauf
      besoin ponctuel
- [ ] Run vert : contrôles OK, 10 tables + `resultat_backtest` écrites
- [ ] Rafraîchissement Power BI enchaîné (jamais sur un run rouge)
