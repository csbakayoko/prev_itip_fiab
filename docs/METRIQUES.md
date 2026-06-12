# Métriques ITIP-FIAB — formalisation

> Réconciliation CPT (comptes) ↔ MRM (revue inventaire). Ce document définit
> **chaque métrique** : la problématique à laquelle elle répond, sa formule, son
> univers de calcul (périmètre / limites) et sa lecture pratique. Il sert de
> contrat pour les fonctions de calcul et les sorties (Excel / JSON / CSV /
> Parquet / base).

---

## 0. Lexique de restitution (vocabulaire client unifié)

Un seul vocabulaire à **deux couches** dans toute la restitution (console,
graphiques, exports). On n'emploie plus « matché » / « mappé » / « MISSING »
côté affichage : ce sont des termes techniques internes.

| Terme affiché | Sens | Remplace (jargon interne) |
|---|---|---|
| **retrouvé** | le dossier de la revue a une contrepartie au compte | matché / mappé |
| **non retrouvé** | le dossier de la revue n'a pas de contrepartie au compte | non mappé / MRM_MISSING / orphelin MRM / « non conforme » (KEEP absent) |
| **conforme** | la consigne est respectée | — |
| **encore au compte** | « à supprimer » non suivie : le dossier devait disparaître mais est retrouvé | « non conforme » (DELETE présent) |

- **Couche 1 — le fait** : *retrouvé* / *non retrouvé* (présence au compte).
- **Couche 2 — le verdict** : *conforme* / KO. Le KO est nommé par le **fait**,
  jamais « non conforme » : pour conserver / ajouter / étudier le KO est
  **« non retrouvé »** (absent du compte) ; pour à supprimer le KO est
  **« encore au compte »** (présent alors qu'il devait disparaître).

Règle de lecture, une phrase : *« retrouvé » = présent au compte ; « conforme »
= consigne respectée (conserver/étudier/ajouter → retrouvé ; à supprimer →
absent)*.

---

## 1. Univers de calcul et règles de population

Toutes les métriques se lisent à partir de `df_result` (sortie du waterfall),
via la colonne `TYPE_RECONCILIATION`. Les catégories :

| Catégorie | `TYPE_RECONCILIATION` | Sens |
|---|---|---|
| Matchés **principale** | `MATCH_EXACT`, `MATCH_WINDOW` | clé nominale complète |
| Matchés **affinée** | `MATCH_TRONC`, `MATCH_TRONC_WINDOW` | nom tronqué 20 car. |
| Matchés **récupération** | `MATCH_IP`, `MATCH_RECHUTE`, `MATCH_RECHUTE_TRONC` | IT→IP, rechutes |
| Récupérés **N+1** | `CPT_LATE` | orphelin CPT retrouvé dans l'inventaire suivant |
| Orphelins MRM | `MRM_MISSING` | dossier MRM sans contrepartie compte |
| Consigne à supprimer | `MRM_DELETE` | MRM marqué « à supprimer » |
| Orphelins compte | `CPT_ONLY` | dossier compte sans contrepartie MRM |
| Obs. tardives IT | `CPT_OBS_TARDIVE` | sinistre clos avant l'inventaire N+1 |
| Récupérés via NON | `CPT_RECUP_NON` | CPT_ONLY repêché sur un MRM statut NON (PM MRM=0) |

**MATCHÉS (inventaire courant)** = principale + affinée + récupération
(`MATCH_LABELS`).

### Règles de population (qui entre dans les calculs)

1. **Statut inventaire NON — repêchage dédié, hors métriques.**
   Le statut `MRM_STATUT_INV = NON` a une PM MRM **toujours = 0** (non remontée à
   la direction financière). Il **n'alimente PAS le matching principal** (fait
   sur les OUI + statut absent). Il sert **uniquement** à une passe de repêchage
   dédiée des `CPT_ONLY` restants (`recover_late_declarations(..., label=`
   `RECUP_NON_LABEL)`, calquée sur le N+1) :
   - Le repêchage (N+1 comme statut NON) **rejoue le waterfall principal dans
     le même ordre et avec les mêmes règles** (`RECOVERY_KEYS` : EXACT →
     WINDOW → TRONC → TRONC_WINDOW → IP → RECHUTE → RECHUTE_TRONC) — aucune
     étape sans contrainte de date : un écart de survenance au-delà des
     fenêtres n'est pas considéré comme la même observation. L'ordre strict →
     flexible garantit qu'un assuré à plusieurs sinistres est rapproché de la
     **bonne** contrepartie (une clé lâche seule choisirait un dossier
     arbitraire au dédoublonnage). L'étape gagnante est tracée dans `LATE_KEY`.
   - Un `CPT_ONLY` retrouvé dans les MRM NON est tagué **`CPT_RECUP_NON`**
     (LATE_SOURCE = `STATUT_NON`) : anomalie résolue, mais **PM MRM = 0** → label
     distinct ⇒ **EXCLU par construction de TOUTES les métriques** (couverture,
     chute, conformité, audit consignes, ratios). Présenté uniquement dans
     l'analyse dédiée `recup_statut_non` (onglet d'export dédié, par clause ×
     consigne × clé) et en ligne séparée de la bulle COMPTE.
   - Un MRM NON qui ne repêche rien n'est **jamais unionné** → il disparaît
     naturellement, **zéro empreinte dans la volumétrie** (pas de `MRM_MISSING`
     en statut NON). Aucune fonction de rejet n'est nécessaire.
   - Conséquence : `MRM_MISSING` ne contient que des OUI ; les métriques de
     valeur ne sont jamais polluées par des couples à PM MRM = 0.
   - **Auto-contrôle** : la synthèse vérifie l'hypothèse « NON ⇒ PM MRM = 0 »
     sur les repêchés (`recup_non_pm_mrm_ok`, restitué console + colonne
     `RECUP_NON_PM_MRM_OK` de `synthese_indicateurs`). Une violation est loguée
     en warning : l'exclusion des métriques ne serait alors plus neutre en valeur.

2. **Observations tardives IT (`CPT_OBS_TARDIVE`)** — sinistres clos avant
   l'inventaire suivant, jamais matchés, **sans** contrepartie MRM. **Exclues**
   de tous les taux et de tous les calculs de PM / chute. Présentées à part
   (volumétrie + PM compte) car explicables, **ce ne sont pas des anomalies**.

3. **Récupérés N+1 (`CPT_LATE`)** — orphelins compte retrouvés dans un
   inventaire ultérieur, **avec** contrepartie MRM. **Inclus** dans l'univers
   métriques (ils ont matché, sur un autre inventaire). Seuls les **OUI** (+
   statut absent) du N+1 sont éligibles : un NON du N+1 (PM MRM = 0) est
   écarté — sinon il entrerait dans l'univers de chute avec une PM nulle. Le
   repêchage statut NON, lui, porte exclusivement sur l'exercice N.

4. **Anomalies = `CPT_ONLY` définitifs** — sans contrepartie MRM, ni récupérés,
   ni explicables.

### Deux univers de référence

- **Univers MRM** (vision « est-ce que la revue est couverte ? ») :
  `MATCHÉS + MRM_MISSING + MRM_DELETE`.
- **Univers compte réconciliable** (vision « le compte est-il justifié ? ») :
  `MATCHÉS + CPT_LATE + CPT_ONLY`. (Obs. tardives IT exclues.)
- **Univers métriques PM / chute** : **tous les retrouvés** (`MATCHÉS +
  CPT_LATE`, **hors statut inventaire NON** uniquement) — les seuls dossiers
  ayant une contrepartie MRM comparable. Identique à la bulle RETROUVÉS :
  « à supprimer » retrouvées et sans-consigne reconnue incluses.

---

## 2. Taux de couverture

### 2.1 Taux de couverture MRM
- **Problématique** : quelle part de la revue MRM a été retrouvée au compte ?
- **Univers** : MRM à comparer = `MATCHÉS + MRM_MISSING` (hors `MRM_DELETE`,
  qui n'a pas vocation à être au compte).
- **Formule** : `nb(MATCHÉS) / nb(MATCHÉS + MRM_MISSING)`.
- **Limite** : en nombre de dossiers (pas en PM). Ne juge pas l'écart de
  provision, seulement la présence.
- **Lecture** : 100 % = toute la revue à conserver/ajouter/étudier a une
  contrepartie compte. Le complément = dossiers MRM non mappés (à instruire).

### 2.2 Taux de couverture compte
- **Problématique** : quelle part du compte réconciliable est justifiée par un
  match à l'inventaire courant ?
- **Univers** : compte réconciliable = `MATCHÉS + CPT_LATE + CPT_ONLY`.
- **Formule** : `nb(MATCHÉS) / nb(MATCHÉS + CPT_LATE + CPT_ONLY)`.
- **Limite** : les récupérés N+1 sont au dénominateur (compte) mais pas au
  numérateur (ils n'ont pas matché l'inventaire *courant*).
- **Lecture** : complément = ce qui n'est pas justifié par l'inventaire courant
  (récupéré ailleurs ou orphelin).

---

## 3. Taux de récupération (déclarations tardives N+1)

### 3.1 Taux de récupération tardive
- **Problématique** : parmi les orphelins compte post-inventaire, combien sont
  rattrapés dans l'inventaire suivant ?
- **Univers** : orphelins post-inventaire = `CPT_LATE + CPT_ONLY`.
- **Formule** : `nb(CPT_LATE) / nb(CPT_LATE + CPT_ONLY)`.
- **Limite** : dépend de la fourniture du fichier MRM N+1 (`FICHIER_MRM_N1`).
  Sans N+1, `CPT_LATE = 0` → taux nul (non « 0 % de performance », mais « non
  mesuré »).

### 3.2 Taux de récupération global
- **Problématique** : au total, quelle part du compte réconciliable est
  justifiée (inventaire courant **ou** N+1) ?
- **Formule** : `nb(MATCHÉS + CPT_LATE) / nb(MATCHÉS + CPT_LATE + CPT_ONLY)`.
- **Lecture** : ≠ couverture compte (qui ne compte pas le N+1 au numérateur).
  Le complément = anomalies `CPT_ONLY` définitives.

---

## 4. Taux de chute (provisionnement) — **cœur de la cohérence**

> Le taux de chute mesure l'écart de provision entre la référence MRM et le
> compte : `chute = (PM_MRM − PM_CPT) / PM_MRM`. Positif = **sous-provisionné**
> (CPT < MRM, risque) ; négatif = **sur-provisionné** (marge).

### 4.1 Définition unique (formule agrégée)
Pour un ensemble de dossiers *E* :

```
taux_chute(E) = Σ_E (PM_MRM − PM_CPT) / Σ_E PM_MRM × 100
```

Formule **agrégée** (somme des écarts / somme des PM), robuste aux valeurs
extrêmes par dossier (on n'agrège jamais une moyenne de ratios).

### 4.2 Univers commun — règle de cohérence
**Décision : un seul univers pour TOUTE chute = TOUS les retrouvés**
(`MATCHÉS` inventaire courant, consignes confondues, + tous les récupérés N+1
`CPT_LATE`), **hors statut inventaire NON uniquement**. Un dossier retrouvé a
une PM MRM comparable → les « à supprimer » **retrouvées** (encore au compte
ou via N+1) et les **sans consigne reconnue** (`MRM_ACTION` null/inconnue)
sont **incluses**. Les « à supprimer » conformes (absentes du compte) n'ont
jamais matché → hors base par construction. C'est ce qui garantit la
cohérence global ↔ par consigne :

```
taux_chute_global = (Σ_consignes_KAS écarts + Σ_hors_consigne écarts)
                    / (Σ_consignes_KAS PM_MRM + Σ_hors_consigne PM_MRM)
```

Comme global et par-consigne partagent **le même univers et la même formule**,
le global est exactement l'agrégat pondéré des consignes KAS plus le bloc
« hors consigne » → réconciliable ligne à ligne.

> ✅ **Résolu** : `compute_synthese` (consigne et global) et `chute_par_clause`
> partagent le même univers (tous les retrouvés, hors statut NON), et un
> auto-contrôle vérifie à chaque run que global == Σ consignes + hors
> consigne (`chute_coherente`, warning sinon). Le bloc hors consigne est tracé
> dans `hors_consigne_nb` / `_pm_mrm` / `_pm_cpt` / `_del_nb`.

### 4.3 Périmètre des consignes
- **KEEP / ADD / STUDY** : chute pertinente (la PM doit être justifiée au
  compte). Entrent dans le taux de chute par consigne **et** dans le global.
- **Sans consigne reconnue** (`MRM_ACTION` null/inconnue) : le dossier a matché,
  sa PM est comparable → **inclus dans le global** (tracé à part, pas de
  consigne à laquelle l'imputer).
- **DELETE retrouvée (encore au compte / N+1)** : **incluse dans le global**
  (le dossier a matché, sa PM pèse réellement au compte), mais **pas de taux
  de chute par consigne** pour DELETE (un écart sur une PM à supprimer ne
  s'interprète pas comme une chute par consigne) — tracée dans le bloc hors
  consigne (`hors_consigne_del_nb`). Le suivi d'application de la consigne
  reste le *taux de suppression effective* (§5.3) ; les DELETE conformes
  (absentes du compte) n'ont jamais matché et ne pèsent dans aucune PM.
- **Statut inventaire NON** : PM MRM = 0, non remonté à la direction
  financière → **exclu du global** (structurellement déjà hors matching ;
  l'exclusion est aussi explicite dans l'univers de chute).

### 4.4 Niveaux de PM
- **PM MRM**, **PM CPT** et **écart** (`PM_MRM − PM_CPT`) sur **l'univers du
  taux de chute global** (tous les retrouvés, hors statut NON) : ce sont les
  **composantes exactes** du taux — `écart / PM_MRM × 100 ==
  taux_chute_global` partout (synthèse, métriques, graphiques). Aucun %
  affiché sur ce bloc (le seul ratio de chute restitué est le global + le
  par-consigne).
- **Bulle RETROUVÉS = base du taux de chute.** La bulle centrale de la
  synthèse (`trouves_nb = match_nb + late_nb`, PM `trouves_pm_mrm` /
  `trouves_pm_cpt`) et la base chute (`metrics_nb`, `metrics_pm_*`) portent
  **le même univers** : le Δ PM de la bulle est exactement le numérateur du
  taux de chute global. Les deux jeux de clés sont exposés (synthèse,
  métriques `NB/PM_*_RETROUVES` et `NB/PM_*_BASE_CHUTE`) et ne divergent que
  si des retrouvés portent un statut inventaire NON (cas structurellement
  absent, contrôlé par l'auto-contrôle de cohérence).

---

## 5. Suivi des consignes

### 5.1 Conformité par consigne — conforme / KO nommé par le fait
- **Problématique** : la consigne MRM a-t-elle été appliquée au compte ?
- **Règle** :

  | Consigne | Retrouvé au compte (matché) | Non retrouvé (orphelin MRM) |
  |---|---|---|
  | KEEP (à conserver) | **CONFORME** | **NON_RETROUVE** (PM attendue absente) |
  | ADD (à ajouter) | **CONFORME** | **NON_RETROUVE** (informatif) |
  | STUDY (à étudier) | **CONFORME** | **NON_RETROUVE** (informatif) |
  | DELETE (à supprimer) | **ENCORE_AU_COMPTE** (suppression non suivie) | **CONFORME** |

  - Le KO est toujours nommé par le **fait constaté**, jamais « non conforme » :
    **NON_RETROUVE** = consigne conserver/ajouter/étudier absente du compte ;
    **ENCORE_AU_COMPTE** = « à supprimer » toujours présente. Le KO **reste au
    dénominateur** du taux de conformité.
- **Univers** : pour KEEP/ADD/STUDY = `MATCHÉS + MRM_MISSING` portant la
  consigne ; pour DELETE = `MATCHÉS + MRM_DELETE`.
- **Formule** : `pct_conformite = nb(conformes) / nb(univers consigne)`.
- **Colonnes dédiées** (`consignes`, `NATURE_KO`) : `nb_conformes`, `nb_ko` —
  avec `nb_total = nb_conformes + nb_ko`.
- **Deux univers, réconciliés à l'affichage.** La restitution sépare
  explicitement les deux périmètres au lieu de les mélanger sur une ligne :
  - **CONFORMITÉ** (inventaire courant) : `total`, `conformes`, `%conf`, reste.
  - **PROVISIONNEMENT** (inventaire + récupérés N+1) : `base`, `dont N+1`,
    `PM MRM`, `PM CPT`, `chute`.
  Pour KEEP / ADD / STUDY les conformes de l'inventaire sont exactement les
  matchés inventaire de la base chute, d'où la réconciliation directe :
  **`base = conformes + dont N+1`** (les récupérés N+1 servent au taux de chute
  et à la volumétrie PM, pas à la conformité). DELETE : base PM non pertinente.
- **Limite** : conformité = présence/absence, indépendante du montant.

### 5.2 Conformité globale
- `nb(conformes KEEP+ADD+STUDY) / nb(univers KEEP+ADD+STUDY)`. Les non
  retrouvés (KEEP/ADD/STUDY confondus) restent au dénominateur. Volumétries
  historisées : `synthese.NB_NON_RETROUVE` et `NB_ENCORE_AU_COMPTE`.

### 5.3 Consignes « à supprimer » non suivies (DELETE matché)
- **Problématique** : PM qui aurait dû disparaître mais toujours au compte.
- **Univers** : `MRM_DELETE ∩ MATCHÉS`.
- **Sorties** : volumétrie + PM par tranche ; *taux de suppression effective* =
  `nb(DELETE orphelins) / nb(DELETE total)`.

---

## 6. Ventilations détaillées (tables d'analyse, ventilées par CLAUSE × TYPE_CLAUSE)

| Table | Problématique | Univers |
|---|---|---|
| `ratios_globaux` | les ratios de tête (chute global, conformité globale, couverture, récupération) avec numérateur/dénominateur explicites | univers de chaque ratio (cf. LECTURE) |
| `suivi_consignes` | conformité + PM + chute par consigne | conformité : matchés + missing ; PM/chute : `MATCHÉS + CPT_LATE` |
| `suivi_consignes_global` | ratios de suivi des consignes toutes clauses confondues | mêmes univers que `suivi_consignes` |
| `taux_chute` | chute par consigne (sous/sur/conforme, poids) | `MATCHÉS + CPT_LATE`, KEEP/ADD/STUDY |
| `taux_chute_par_clause` | chute globale par clause (consignes confondues, poids PM) | `MATCHÉS + CPT_LATE`, KEEP/ADD/STUDY |
| `consignes_pm` | chute par consigne × catégorie × tranche PM | `MATCHÉS + CPT_LATE`, KEEP/ADD/STUDY |
| `provisionnement` | sous/sur/conforme par consigne | `MATCHÉS + CPT_LATE`, KEEP/ADD/STUDY |
| `ecarts_tranches` | distribution des écarts par tranche € | sous/sur-provisionnés |
| `delete_non_suivies` | DELETE matché (PM non supprimée) | `MRM_DELETE ∩ MATCHÉS` |
| `ventilation_cpt_only` | concentration PM des orphelins compte | `CPT_ONLY` |
| `obs_tardives` | sinistres clos avant inventaire N+1 | `CPT_OBS_TARDIVE` |
| `recup_statut_non` | CPT récupérés via MRM statut NON : conformité par consigne × étape de repêchage | `CPT_RECUP_NON` |
| `recup_statut_non_detail` | enjeu PM compte des récupérés NON (mois de survenance × garantie × tranche PM) | `CPT_RECUP_NON` |

---

## 7. Cohérence d'agrégation (multi-base / multi-inventaire)

- **Intégration N+1** : les `CPT_LATE` sont des dossiers de l'inventaire suivant
  rapatriés ; ils entrent dans l'univers métriques `MATCHÉS + CPT_LATE`
  **partout** (chute, niveaux de PM, récupération). Aucun calcul de chute ne
  doit les omettre (sinon incohérence global ↔ consigne, cf. §4.2).
- **Additivité par clause** : chaque table est ventilée par
  `(CLAUSE, TYPE_CLAUSE)` ; les % sont calculés **dans le scope de chaque
  clause** (fenêtre partitionnée). La somme des numérateurs/dénominateurs par
  clause = total → on peut agréger plusieurs clauses/bases sans recompter.
- **Invariant de cohérence** : `compute_synthese` vérifie que toute ligne tombe
  dans exactement une catégorie connue (`classified_rows == total_rows`) ; sinon
  un `TYPE_RECONCILIATION` inattendu est signalé.

---

## 8. Sorties

### 8.1 Formats
- **Excel** (présentation) : un classeur multi-onglets, un onglet par table +
  un onglet « synthèse » lisible (indicateurs cadrés).
- **CSV** / **Parquet** : une table par fichier (rejeu / dataviz).
- **JSON** : une ligne par enregistrement, par table (`export_json`). ✅
- **Base de données** : table Delta metastore (une par analyse).

### 8.2 Ce qui va en base (proposition à valider)
- Tables ventilées par clause (`suivi_consignes`, `taux_chute`,
  `consignes_pm`, `provisionnement`, `delete_non_suivies`,
  `ventilation_cpt_only`, `obs_tardives`).
- Une table **`synthese_indicateurs`** ✅ : les scalaires (taux de couverture,
  récupération, chute global, conformité, niveaux de PM, volumétries), une ligne
  par run (date d'inventaire + libellé périmètre) → historisation et suivi dans
  le temps. Construite par `kpi_export.build_synthese_indicateurs`, incluse dans
  `collect_analyses` (donc restituée + exportée tous formats + Delta).

### 8.3 Qualité « présentation »
- Libellés explicites, ordre stable, pourcentages arrondis à 0,1.
- Chaque sortie porte `CLAUSE`, `TYPE_CLAUSE` en tête + date d'inventaire.
- Une page de synthèse condensée (3 indicateurs clés : couverture, récupération,
  chute) directement copiable en slide.

---

## 9. Décisions — tranchées

1. **Taux de chute global** = **KEEP + ADD + STUDY** uniquement ; DELETE suivi à
   part (taux de suppression effective). ✅
2. **Univers chute unique = `MATCHÉS + CPT_LATE`** des deux côtés (global et par
   consigne) — incohérence corrigée (`_filter_matched_keep_add_study` et
   `compute_synthese.consigne` alignés). ✅
3. **Synthèse en base** : table `synthese_indicateurs` (1 ligne / run). ✅
4. **JSON** : format ajouté en sortie. ✅
