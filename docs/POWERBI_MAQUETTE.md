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
- **Tables** : les 9 `metrique_*` (dont `metrique_dim_run`) +
  `resultat_backtest`. Noms **stables** —
  le run est porté par les colonnes, jamais par le nom de table. Une table =
  un sujet complet ; les angles d'analyse sont des **colonnes** (`EXERCICE`,
  `AXE`, `SEGMENT`, `UNIVERS`) : le rapport **filtre**, il n'assemble pas.
- **Modèle en étoile — livré clé en main.** Chaque table porte la colonne
  **`CLE_RUN`** (`« <date ISO>|<périmètre> »`) ; `metrique_dim_run` en est le
  **pivot** (une ligne par run : date, année, vision CPT, périmètre, présence
  d'un N+1). Relations à créer : `metrique_dim_run[CLE_RUN]` **(1) → (n)**
  `CLE_RUN` de **chaque** autre table (`metrique_*` et `resultat_backtest`),
  filtre unidirectionnel. Résultat : **un seul segment (date d'inventaire ou
  année, posé sur `dim_run`) pilote tout le rapport** — aucune table de dates
  à fabriquer, aucune clé composite à concaténer dans Power Query.
- **Tri des axes** : dans `metrique_chute` et `metrique_orphelins`, trier la
  colonne `SEGMENT` par la colonne `ORDRE` (« Trier par colonne ») — les
  tranches d'écart, blocs d'ancienneté et rangs s'affichent alors dans
  l'ordre métier, pas alphabétique.
- **Migration (une fois)** : deux cas. Depuis l'ancien export éclaté en 22
  tables : les anciennes `metrique_*` ne sont plus alimentées (et
  `metrique_consignes` change de schéma) → `DROP TABLE` sur les anciennes.
  Depuis des tables regroupées créées **avant** `CLE_RUN` / `ORDRE` /
  « Tranche d'écart » : le schéma a évolué et l'écriture historisée refuse un
  changement de schéma → `DROP TABLE` sur les `metrique_*` existantes (et
  `resultat_backtest`). Dans les deux cas les 9 tables se (re)créent au run
  suivant ; rebrancher les visuels selon la maquette ci-dessous.
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
(toutes pages, posés sur `metrique_dim_run` via les relations `CLE_RUN`) :
`DATE_INVENTAIRE` (ou `ANNEE_INVENTAIRE`), `PERIMETRE` ; segments locaux :
`TYPE_COMPTE`, `CLAUSE` là où l'axe existe. Les PNG `1_…` à `12_…` du dossier
`graphiques` donnent, pour chaque page, le rendu cible du visuel principal.

### P1 — Synthèse (la page d'atterrissage)
- **Cartes KPI** (une ligne de `metrique_synthese`) : taux de couverture MRM,
  couverture compte, récupération globale, **taux de chute inventaire**,
  conformité globale. Sous chaque carte : le nb et la PM de l'univers
  (`NB/PM_*_BASE_CHUTE` vs `NB/PM_*_RETROUVES` — ne pas mélanger, cf.
  METRIQUES §4.4).
- **Décomposition du compte** (`metrique_couverture`, `UNIVERS` = « Compte ») :
  barres empilées retrouvés / N+1 / repêchés / clos / anomalies (graphe 1).
- **Historique** : `metrique_synthese` non filtrée par date — courbe des KPI
  par `DATE_INVENTAIRE` (2023 vs 2024…), rendue possible par `dim_run`.

### P2 — Taux de chute
- Tout vient de `metrique_chute`, par filtre sur `AXE` : cartes taux
  inventaire + composantes PM (`AXE` = « Ensemble », graphe 7), taux N+1 en
  regard, clairement séparé (« analyse séparée ») ; barres par type de compte
  (`AXE` = « Type de compte », bloc `EXERCICE` = « Inventaire courant »
  seul — graphe 3) ; barres par ancienneté (`AXE` = « Ancienneté » : N / N-1
  / N-2 et antérieur — graphe 10, tri par `ORDRE`).
- Barres par consigne : `metrique_consignes`, `EXERCICE` = « Inventaire
  courant », lignes à PM pertinente (graphes 4 et 9).
- Axe couleur : signe de la chute (rouge sous-provisionné / vert marge).

### P3 — Écarts par dossier (distribution) 🆕
- **La question** : combien de dossiers sur/sous-provisionnés, à quelle
  ampleur ? Tout vient de `metrique_chute`, `AXE` = « Tranche d'écart »,
  bloc `EXERCICE` = « Inventaire courant » (graphe 12).
- **Histogramme** (barres) : `SEGMENT` en axe (tri par `ORDRE` — du plus
  sur-provisionné au plus sous-provisionné), `NB_DOSSIERS` en valeur,
  couleur par sens (sur-provisionné / écart nul / sous-provisionné).
- **Cartes** : Σ `NB_SOUS_PROVISION` (dossiers en risque), Σ
  `NB_SUR_PROVISION` (marge), Σ `NB_ECART_NUL` — sommes autorisées
  (colonnes additives).
- **Table d'appui** : par tranche — `NB_DOSSIERS`, `PM_MRM`, `PM_CPT`,
  `ECART` (l'enjeu en € de chaque tranche, pas seulement la volumétrie).
- Segment local : seuils fixes (les tranches viennent du moteur,
  `SEUILS_ECART_PM` — le rapport n'invente pas de découpage).

### P4 — Suivi des consignes (le tableau de bord)
- **Matrice** `metrique_consignes_par_type_compte` : TYPE_COMPTE × CONSIGNE —
  nb, suivies / non suivies, PM, `NB_NON_REMONTE_DF`.
- Barres 100 % par consigne (`metrique_consignes`, `EXERCICE` = « Inventaire
  courant ») : conforme / non retrouvé / encore au compte (graphe 5) ; carte
  conformité globale (`CONFORMITE_GLOBALE_PCT` de `metrique_synthese`,
  graphe 8).

### P5 — Couverture et bilan cas par cas
- `metrique_couverture`, `UNIVERS` = « Revue MRM » : part de la revue
  retrouvée, non retrouvés par consigne (graphe 2).
- **Table `metrique_bilan_cas`** telle quelle (nb, PM, taux, EXPLICATION) :
  c'est LE tableau de restitution — chaque ligne a sa phrase d'explication.

### P6 — Orphelins compte (investigation)
- Tout vient de `metrique_orphelins`, par filtre sur `AXE` : « Type de
  compte » = la ventilation complète (le total des orphelins s'y lit) ;
  « Clause (détail) » trié par `ORDRE` (1 = à investiguer en premier avec le
  souscripteur — graphe 11) — **angle de détail**, il ne couvre que les
  comptes porteurs d'une clause, son total est donc inférieur : ne pas s'en
  servir pour un cumul ; « Garantie » / « Ancienneté » ; « Composante de clé
  nulle » (pourquoi ça n'a pas matché) ; « Mois de survenance » (effet fin
  d'année : `ORDRE` ≥ 10 = Oct-Déc — graphe 6).

### P7 — Analyses séparées : N+1 et statut NON
- `metrique_chute` (`AXE` = « Ensemble », les deux blocs `EXERCICE` en
  regard), `metrique_consignes` (bloc `EXERCICE` = « Récupérés N+1 »).
  Bandeau rappel : « hors stats globales » (METRIQUES §4.2). La colonne
  `AVEC_MRM_N1` de `metrique_dim_run` explique un bloc vide (run sans N+1).

### P8 — Fiabilité du run
- Table `metrique_controles_coherence` (CONTROLE / ATTENDU / OBTENU / OK,
  icône ✔/✘) + carte « x/x contrôles OK ». La page qui justifie que les
  onglets se recoupent.

### Drillthrough — détail tête par tête
- Page masquée sur `resultat_backtest` (relié à `dim_run` par `CLE_RUN`,
  comme les tables métriques) : depuis toute ligne clause / consigne /
  catégorie, clic droit ▸ extraire → liste des dossiers
  (`TYPE_RECONCILIATION`, PM des deux côtés, clé gagnante, dimensions).

## 4. Rafraîchissement

1. Job Databricks `itip_fiab_powerbi` (bloquant sur les contrôles — un Job
   vert = des tables cohérentes).
2. Refresh du dataset Power BI (planifié après le Job, ou déclenché par
   l'orchestrateur).
3. Rejouer un inventaire remplace ses lignes (`DATE_INVENTAIRE ×
   PERIMETRE`) : le rapport n'a **jamais** de doublon de run.

## 5. Tutoriels

La mise en œuvre pas à pas :

- [`TUTORIEL_JOB_DATABRICKS.md`](TUTORIEL_JOB_DATABRICKS.md) — **la source de
  référence** : construire, lancer, planifier et dépanner le Job qui alimente
  ces tables (les exports Word / PDF de `livrables/tutoriels/` en dérivent) ;
- `livrables/tutoriels/Tutoriel_PowerBI_Backtest_ITIP.docx` — construire le
  rapport : connexion SQL Warehouse, modèle en étoile (`dim_run` × `CLE_RUN`),
  mesures DAX autorisées, pages P1→P8 + drillthrough, thème JSON (annexe C) ;
  version deck en `.pptx` (16 diapos) ;
- `livrables/tutoriels/PROMPT_CLAUDE_DESIGN.md` — le prompt de génération de
  la **maquette HTML dynamique** du rapport (visuels 100 % natifs Power BI,
  jeu de données fictif) pour valider la mise en page avant de construire.
