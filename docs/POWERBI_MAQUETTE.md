# Rapport Power BI « Backtest Prévoyance ITIP » — maquette et branchement

> Cible de restitution de l'étude : le rapport Power BI **« Backtest
> Prévoyance ITIP »**, branché sur les
> tables Delta du schéma `hive_metastore.itip_backtest` (SQL Warehouse
> Databricks). Le rapport **ne calcule rien** : toutes les grandeurs viennent
> des tables `metrique_*` (contrat : [`METRIQUES.md`](METRIQUES.md) §6/§8) —
> il tope dedans, filtre, met en forme. Un rafraîchissement = relire les
> tables après le Job `itip_fiab_powerbi`.

---

## 1. Connexion et modèle de données

- **Connecteur** : Azure Databricks ▸ SQL Warehouse, mode **Import**
  (volumétrie faible : tables agrégées ; le refresh suit le Job).
  `resultat_backtest` (détail tête par tête) peut rester en DirectQuery si
  la volumétrie grossit.
- **Tables** : les 8 `metrique_*` + `resultat_backtest`. Noms **stables** —
  le run est porté par les colonnes, jamais par le nom de table. Une table =
  un sujet complet ; les angles d'analyse sont des **colonnes** (`EXERCICE`,
  `AXE`, `SEGMENT`, `UNIVERS`) : le rapport **filtre**, il n'assemble pas.
- **Migration (une fois)** : l'export était auparavant éclaté en 22 tables ;
  les anciennes `metrique_*` ne sont plus alimentées (et `metrique_consignes`
  change de schéma) → passer un `DROP TABLE` sur les anciennes, les 8 tables
  se créent au run suivant, puis rebrancher les visuels selon la maquette
  ci-dessous.
- **Clé de run** : chaque table porte `DATE_INVENTAIRE` / `PERIMETRE` /
  `LIBELLE_RUN`. Modèle en étoile minimal : une table de dates
  (`DimInventaire` = les `DATE_INVENTAIRE` distinctes de `metrique_synthese`)
  reliée en 1-n à toutes les tables → **un seul segment (slicer) de date
  d'inventaire pilote tout le rapport**. Idem `PERIMETRE` (défaut : `MULTI`).
- **Aucune mesure DAX de recalcul métier** : les taux sont des colonnes des
  tables (formules agrégées, cf. METRIQUES §4.1) — recalculer un ratio en
  DAX (moyenne de ratios…) casserait la cohérence garantie par
  `controles_coherence`. Les seules mesures autorisées : `SELECTEDVALUE` /
  sommes de colonnes déjà additives (nb, PM, écarts).

## 2. Vocabulaire et sémantique visuelle

Lexique client à deux couches (METRIQUES §0) — jamais de jargon interne
(`matché`, `MISSING`) dans les libellés du rapport :

| Notion | Libellé affiché | Couleur |
|---|---|---|
| Consigne respectée | **conforme** | vert |
| KEEP/ADD/STUDY absent du compte | **non retrouvé** | orange |
| DELETE toujours présent | **encore au compte** | rouge |
| Chute > 0 (CPT < MRM) | **sous-provisionné** | rouge |
| Chute < 0 (CPT > MRM) | **sur-provisionné (marge)** | vert |
| Explicable (obs. tardives, N+1, statut NON) | libellé du cas | gris/bleu neutre |

Style : reprendre la charte du deck `Restitution_BackTest_ITIP.pptx`
(mêmes intitulés de sections → le rapport et la restitution orale racontent
la même histoire).

## 3. Maquette — une page par question métier

Chaque page = une question, ses visuels, sa table source. Segments globaux
(toutes pages) : `DATE_INVENTAIRE`, `PERIMETRE`, `TYPE_COMPTE`, `CLAUSE`.

### P1 — Synthèse (la page d'atterrissage)
- **Cartes KPI** (une ligne de `metrique_synthese`) : taux de couverture MRM,
  couverture compte, récupération globale, **taux de chute inventaire**,
  conformité globale. Sous chaque carte : le nb et la PM de l'univers
  (`NB/PM_*_BASE_CHUTE` vs `NB/PM_*_RETROUVES` — ne pas mélanger, cf.
  METRIQUES §4.4).
- **Décomposition du compte** (`metrique_couverture`, `UNIVERS` = « Compte ») :
  barres empilées retrouvés / N+1 / repêchés / clos / anomalies.
- **Historique** : `metrique_synthese` non filtrée par date — courbe des KPI
  par `DATE_INVENTAIRE` (2023 vs 2024…).

### P2 — Taux de chute
- Tout vient de `metrique_chute`, par filtre sur `AXE` : cartes taux
  inventaire + composantes PM (`AXE` = « Ensemble »), taux N+1 en regard,
  clairement séparé (« analyse séparée ») ; barres par type de compte
  (`AXE` = « Type de compte », bloc `EXERCICE` = « Inventaire courant »
  seul) ; barres par ancienneté (`AXE` = « Ancienneté » : N / N-1 / N-2 et
  antérieur).
- Barres par consigne : `metrique_consignes`, `EXERCICE` = « Inventaire
  courant », lignes à PM pertinente.
- Axe couleur : signe de la chute (rouge sous-provisionné / vert marge).

### P3 — Suivi des consignes (le tableau de bord)
- **Matrice** `metrique_consignes_par_type_compte` : TYPE_COMPTE × CONSIGNE —
  nb, suivies / non suivies, PM, `NB_NON_REMONTE_DF`.
- Barres 100 % par consigne (`metrique_consignes`, `EXERCICE` = « Inventaire
  courant ») : conforme / non retrouvé / encore au compte ; carte conformité
  globale (`CONFORMITE_GLOBALE_PCT` de `metrique_synthese`).

### P4 — Couverture et bilan cas par cas
- `metrique_couverture`, `UNIVERS` = « Revue MRM » : part de la revue
  retrouvée, non retrouvés par consigne.
- **Table `metrique_bilan_cas`** telle quelle (nb, PM, taux, EXPLICATION) :
  c'est LE tableau de restitution — chaque ligne a sa phrase d'explication.

### P5 — Orphelins compte (investigation)
- Tout vient de `metrique_orphelins`, par filtre sur `AXE` : « Type de
  compte » = la ventilation complète (le total des orphelins s'y lit) ;
  « Clause (détail) » trié par `ORDRE` (1 = à investiguer en premier avec le
  souscripteur) — **angle de détail**, il ne couvre que les comptes porteurs
  d'une clause, son total est donc inférieur : ne pas s'en servir pour un
  cumul ; « Garantie » / « Ancienneté » ; « Composante de clé nulle »
  (pourquoi ça n'a pas matché) ; « Mois de survenance » (effet fin d'année :
  `ORDRE` ≥ 10 = Oct-Déc).

### P6 — Analyses séparées : N+1 et statut NON
- `metrique_chute` (`AXE` = « Ensemble », les deux blocs `EXERCICE` en
  regard), `metrique_consignes` (bloc `EXERCICE` = « Récupérés N+1 »).
  Bandeau rappel : « hors stats globales » (METRIQUES §4.2).

### P7 — Fiabilité du run
- Table `metrique_controles_coherence` (CONTROLE / ATTENDU / OBTENU / OK,
  icône ✔/✘) + carte « x/x contrôles OK ». La page qui justifie que les
  onglets se recoupent.

### Drillthrough — détail tête par tête
- Page masquée sur `resultat_backtest` : depuis toute ligne clause /
  consigne / catégorie, clic droit ▸ extraire → liste des dossiers
  (`TYPE_RECONCILIATION`, PM des deux côtés, clé gagnante, dimensions).

## 4. Rafraîchissement

1. Job Databricks `itip_fiab_powerbi` (bloquant sur les contrôles — un Job
   vert = des tables cohérentes).
2. Refresh du dataset Power BI (planifié après le Job, ou déclenché par
   l'orchestrateur).
3. Rejouer un inventaire remplace ses lignes (`DATE_INVENTAIRE ×
   PERIMETRE`) : le rapport n'a **jamais** de doublon de run.

## 5. Tutoriels (livrables/)

La mise en œuvre pas à pas de cette maquette est documentée dans
[`livrables/`](../livrables/README.md) (Word / PPT / PDF) :

- `Tutoriel_PowerBI_Backtest_ITIP.docx` — construire le rapport : connexion
  SQL Warehouse, modèle en étoile, mesures DAX autorisées, pages P1→P7 +
  drillthrough, thème JSON (annexe C) ;
- `Tutoriel_PowerBI_Backtest_ITIP.pptx` — la version deck (16 diapos) ;
- `Tutoriel_Job_Databricks_ITIP.docx` — créer, lancer, planifier et dépanner
  le Job qui alimente ces tables.
