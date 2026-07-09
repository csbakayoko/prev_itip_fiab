# Rapport Power BI — maquette et branchement

> Cible de restitution de l'étude : un rapport Power BI branché sur les
> tables Delta du schéma `hive_metastore.itip_fiab` (SQL Warehouse
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
- **Tables** : les 21 `metrique_*` + `resultat_backtest`. Noms **stables** —
  le run est porté par les colonnes, jamais par le nom de table.
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
- **Décomposition du compte** (`metrique_compte_justification`) : barres
  empilées retrouvés / N+1 / repêchés / clos / anomalies.
- **Historique** : `metrique_synthese` non filtrée par date — courbe des KPI
  par `DATE_INVENTAIRE` (2023 vs 2024…).

### P2 — Taux de chute
- Cartes : taux inventaire + composantes PM (`metrique_taux_chute`), taux
  N+1 en regard, clairement séparé (« analyse séparée »).
- Barres par clause (`metrique_chute_par_clause`, bloc EXERCICE =
  « Inventaire courant » seul) ; barres par ancienneté
  (`metrique_chute_par_anciennete` : N / N-1 / N-2 et antérieur) ; barres
  par consigne (`metrique_chute_par_consigne`).
- Axe couleur : signe de la chute (rouge sous-provisionné / vert marge).

### P3 — Suivi des consignes (le tableau de bord)
- **Matrice** `metrique_consignes_par_clause` : TYPE_COMPTE × CLAUSE ×
  CONSIGNE — nb, suivies / non suivies, PM, `NB_NON_REMONTE_DF`.
- Barres 100 % par consigne (`metrique_consignes`) : conforme / non
  retrouvé / encore au compte ; cartes conformité globale
  (`metrique_conformite_globale`).

### P4 — Couverture et bilan cas par cas
- `metrique_couverture_mrm` : part de la revue retrouvée, non retrouvés par
  consigne.
- **Table `metrique_bilan_cas`** telle quelle (nb, PM, taux, EXPLICATION) :
  c'est LE tableau de restitution — chaque ligne a sa phrase d'explication.

### P5 — Orphelins compte (investigation)
- `metrique_orphelins_par_clause` trié par `RANG` (1 = à investiguer en
  premier avec le souscripteur) ; ventilations garantie / ancienneté ;
  `metrique_orphelins_cles_nulles` (pourquoi ça n'a pas matché) ;
  `metrique_anomalies_cpt_only` par mois (effet fin d'année).

### P6 — Analyses séparées : N+1 et statut NON
- `metrique_chute_par_exercice` (inventaire courant vs récupérés N+1),
  `metrique_suivi_n1` (consignes N+1). Bandeau rappel : « hors stats
  globales » (METRIQUES §4.2).

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
