# Métriques ITIP-FIAB — formalisation

> Réconciliation CPT (comptes) ↔ MRM (revue inventaire). Ce document définit
> **chaque métrique** : la problématique à laquelle elle répond, sa formule, son
> univers de calcul (périmètre / limites) et sa lecture pratique. Il sert de
> contrat pour les fonctions de calcul et les sorties (Excel / JSON / CSV /
> Parquet / base).
>
> 📘 **Pour une lecture pédagogique avec exemples chiffrés** (interprétation
> orale, intérêt, limites de chaque KPI) → [`GUIDE_KPI.md`](GUIDE_KPI.md).

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
| Matchés **clé clause** | `MATCH_CLAUSE`, `MATCH_CLAUSE_WINDOW`, `MATCH_CLAUSE_TRONC`, `MATCH_CLAUSE_TRONC_WINDOW` | clé de secours : n° de clause à la place du RPP (RPP compte nul / mal renseigné) |
| Récupérés **N+1** | `CPT_LATE` | orphelin CPT retrouvé dans l'inventaire suivant |
| Orphelins MRM | `MRM_MISSING` | dossier MRM sans contrepartie compte |
| Consigne à supprimer | `MRM_DELETE` | MRM marqué « à supprimer » |
| Orphelins compte | `CPT_ONLY` | dossier compte sans contrepartie MRM |
| Obs. tardives IT | `CPT_OBS_TARDIVE` | sinistre clos avant l'inventaire N+1 |
| Récupérés via NON | `CPT_RECUP_NON` | CPT_ONLY repêché sur un MRM statut NON (PM MRM=0) |

**MATCHÉS (inventaire courant)** = principale + affinée + récupération + clé
clause (`MATCH_LABELS`). Les matchs sur **clé clause** ont une vraie contrepartie
MRM (RPP compte absent / non fiable, retrouvé via le n° de clause + nom + dates) →
ils entrent dans l'univers de chute comme les autres ; bucket distinct, suivi à
part dans `bilan_cas` (ligne « Clé clause ») et `synthese` (`NB_MATCH_CLAUSE`) pour
auditer cette clé moins stricte.

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
   - Le repêchage statut NON porte sur **les deux exercices** : le NON de
     l'exercice N **et** le NON du N+1 (un NON N+1 a aussi une PM MRM = 0, donc
     hors métriques — mais le dossier est bien au compte, on en donne la trace).
     LATE_SOURCE distingue l'origine : `STATUT_NON` (exercice N) /
     `STATUT_NON_N1` (exercice N+1). La part de chaque exercice est ventilée dans
     `bilan_cas` (deux sous-lignes + total) et `synthese`
     (`NB_RECUP_NON_N` / `NB_RECUP_NON_N1`).
   - Un `CPT_ONLY` retrouvé dans les MRM NON est tagué **`CPT_RECUP_NON`** :
     anomalie résolue, mais **PM MRM = 0** → label distinct ⇒ **EXCLU par
     construction de TOUTES les métriques** (couverture, chute, conformité, audit
     consignes, ratios). Présenté uniquement dans l'analyse dédiée
     `recup_statut_non` (par clause × consigne × clé) et en ligne séparée de la
     bulle COMPTE.
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
   statut absent) du N+1 produisent des `CPT_LATE` : un NON du N+1 (PM MRM = 0)
   n'en est pas un — sinon il entrerait dans l'univers de chute avec une PM
   nulle. Le NON du N+1 n'est pas perdu pour autant : il rejoint la passe de
   repêchage statut NON (`CPT_RECUP_NON`, hors métriques, cf. règle 1, ventilé
   N / N+1).

4. **Anomalies = `CPT_ONLY` définitifs** — sans contrepartie MRM, ni récupérés,
   ni explicables.

### Deux univers de référence

- **Univers MRM** (vision « est-ce que la revue est couverte ? ») :
  `MATCHÉS + MRM_MISSING + MRM_DELETE`.
- **Univers compte réconciliable** (vision « le compte est-il justifié ? ») :
  `MATCHÉS + CPT_LATE + CPT_ONLY`. (Obs. tardives IT exclues.)
- **Univers métriques PM / chute (stats globales)** : `MATCHÉS` (inventaire
  courant) **hors consigne « à supprimer » et hors statut inventaire NON**
  (sans-consigne reconnue inclus). Les **récupérés N+1** (`CPT_LATE`) sont
  une **analyse séparée, hors stats globales** : leur propre taux de chute
  et leur propre suivi de consignes (cf. §4.2).

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

### 4.2 Un taux principal, des analyses séparées — règle de cohérence
**Décision : les stats globales de chute = l'inventaire courant seul.**

1. **LE taux de chute** (`taux_chute_inventaire`) : matchés du MRM de
   l'exercice courant, hors « à supprimer » / statut NON. C'est le taux de
   la revue auditée — **toutes les grandeurs globales (niveaux de PM, écart,
   graphes 3 et 7) se calculent sur cet univers**.
2. **Taux de chute N+1** (`taux_chute_n1`, **analyse séparée, HORS stats
   globales**) : récupérés `CPT_LATE`, hors « à supprimer » (consigne N+1).
   Accompagné de son propre **suivi des consignes N+1** (`n1_consignes` :
   KEEP/ADD/STUDY = conformes, DELETE = encore au compte). Ses métriques se
   lisent dans les blocs dédiés (`chute_n1_*`, tables `chute_par_exercice` /
   `suivi_n1`, bloc N+1 de `chute_par_clause`).

Les autres exclus gardent leur **analyse à part** : « à supprimer »
retrouvées → conformité (« encore au compte ») + taux de suppression
effective (§5.3) ; repêchés statut NON → analyse dédiée `recup_statut_non`
(§1) — comme les N+1, ils ne rentrent pas dans les stats globales.

Le **par-consigne** (table consignes, graphes 4/5/9) porte exclusivement
l'**exercice courant** : la consigne d'un récupéré N+1 vient de l'inventaire
N+1, pas de la revue auditée — lui attribuer une conformité ou une chute dans
la table principale prêterait à la revue courante des décisions qu'elle n'a
pas prises.

```
taux_chute_inventaire = (Σ_consignes_KAS écarts + Σ_sans_consigne écarts)
                        / (Σ_consignes_KAS PM_MRM + Σ_sans_consigne PM_MRM)
taux_chute_n1         = écart_N+1 / PM_MRM_N+1          (analyse séparée)
```

> ✅ **Résolu** : auto-contrôle à chaque run (`chute_coherente`, warning
> sinon) — chute == Σ consignes KAS + sans consigne (même univers : matchés
> inventaire courant hors « à supprimer » / statut NON). Le bloc sans
> consigne est tracé dans `hors_consigne_*` (inventaire) et
> `n1_sans_consigne` (N+1) ; les deux exercices dans `metrics_*` /
> `chute_n1_*` (table `chute_par_exercice`).

La ventilation **par clause** (`chute_par_clause`, graphe 3) porte la même
séparation : deux blocs `EXERCICE` (« Inventaire courant » = stats globales /
« Récupérés N+1 » = analyse séparée). Dans chaque bloc, Σ clauses (Σ écart /
Σ PM MRM) == le taux correspondant du §4.2, et les poids PM se lisent dans
le bloc. Le graphe 3 ne trace que le bloc inventaire.

La ventilation **par ancienneté** (`chute_par_anciennete`, graphe 10) découpe
le même univers par **année de survenance** relative à l'inventaire :
**N / N-1 / N-2 et antérieur** (bloc `Indéterminée` si la survenance est nulle
ou l'inventaire non daté). Le découpage est métier : **la méthode d'inventaire
diffère selon l'année** (revue tête par tête sur N-1). Même structure que par
clause (deux blocs `EXERCICE`, taux/poids dans le bloc, Σ bloc inventaire ==
taux principal) ; année dérivée de `year(CPT_D_SURVENANCE)`, référence
`d["date_inventaire"]`.

### 4.3 Périmètre des consignes
- **KEEP / ADD / STUDY** : chute pertinente (la PM doit être justifiée au
  compte). Entrent dans le taux de chute par consigne **et** dans le taux
  principal.
- **Sans consigne reconnue** (`MRM_ACTION` null/inconnue) : le dossier a matché,
  sa PM est comparable → **inclus dans la base chute** (tracé à part, pas de
  consigne à laquelle l'imputer).
- **DELETE** : la PM aurait dû être **supprimée** ; un écart « PM_MRM −
  PM_CPT » n'a pas le sens d'une chute. → **exclue de tous les taux de
  chute** (inventaire comme N+1), qu'elle soit retrouvée au compte ou
  portée par un N+1. Analyse à part : conformité (« encore au compte ») et
  *taux de suppression effective* (§5.3) ; les DELETE conformes (absentes du
  compte) n'ont jamais matché et ne pèsent dans aucune PM.
- **Statut inventaire NON** : PM MRM = 0, non remonté à la direction
  financière → **exclu de la base chute** (structurellement déjà hors
  matching ; l'exclusion est aussi explicite dans l'univers de chute).

### 4.4 Niveaux de PM
- **PM MRM**, **PM CPT** et **écart** (`PM_MRM − PM_CPT`) sur **l'univers du
  taux de chute** (matchés inventaire courant) : ce sont les **composantes
  exactes** du taux — `écart / PM_MRM × 100 == taux_chute_inventaire`
  partout (synthèse, métriques, graphiques). Aucun % affiché sur ce bloc
  (les ratios restitués sont les taux du §4.2 + le par-consigne). Les
  niveaux de PM des récupérés N+1 sont dans leur bloc séparé (`chute_n1_*`).
- **Bulle RETROUVÉS ≠ base du taux de chute.** La bulle centrale montre
  **tous les retrouvés** (`trouves_nb = match_nb + late_nb`, PM
  `trouves_pm_mrm` / `trouves_pm_cpt`) : c'est la vision « justification du
  compte ». La **base chute** (`metrics_nb`, `metrics_pm_*`) = matchés
  inventaire courant hors « à supprimer » / statut NON, détaillée dans le
  bloc NIVEAUX DE PM. Les deux jeux de clés sont exposés
  (`NB/PM_*_RETROUVES` et `NB/PM_*_BASE_CHUTE`) ; leur différence = les
  « à supprimer » retrouvées + les récupérés N+1.

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
- **Univers** : EXERCICE COURANT pur — pour KEEP/ADD/STUDY = `MATCHÉS +
  MRM_MISSING` portant la consigne ; pour DELETE = `MATCHÉS + MRM_DELETE`.
  Les récupérés N+1 n'y participent pas (consigne d'un autre exercice) :
  leur suivi est SÉPARÉ (`n1_consignes`, table `suivi_n1` — KEEP/ADD/STUDY
  conformes, DELETE encore au compte).
- **Formule** : `pct_conformite = nb(conformes) / nb(univers consigne)`.
- **Colonnes dédiées** (`consignes`, `NATURE_KO`) : `nb_conformes`, `nb_ko` —
  avec `nb_total = nb_conformes + nb_ko`.
- **Deux lectures, même exercice.** La restitution sépare explicitement les
  deux lectures au lieu de les mélanger sur une ligne — toutes deux sur
  l'**inventaire courant pur** :
  - **CONFORMITÉ** : `total`, `conformes`, `%conf`, reste.
  - **PROVISIONNEMENT** : `base` (matchés de la consigne), `PM MRM`,
    `PM CPT`, `chute`.
  Pour KEEP / ADD / STUDY la base PM est exactement les conformes (matchés
  inventaire courant de la consigne) : **`base = conformes`**. DELETE : base
  PM non pertinente. Les récupérés N+1 n'apparaissent nulle part ici (suivi
  et chute séparés, bloc N+1).
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

## 6. Tables métriques exportées (core/metrics/__init__.py — toutes_metriques)

Les 20 tables pandas tidy écrites par `export_metriques` (CSV / JSON /
Parquet / Excel / Delta) :

> **Schéma standard des exports** : chaque table porte les colonnes de run
> `DATE_INVENTAIRE` et `LIBELLE_RUN` ; en Delta, chaque table est **historisée**
> par `DATE_INVENTAIRE` (partition remplacée par le run, les autres inventaires
> coexistent). Les lignes ventilées portent `TYPE_COMPTE` (PB / HPB / préfixe
> brut si type non mappé — jamais null muet) et `CLAUSE` (nullable : un compte
> sans n° de clause est une donnée légitime).

| Table | Problématique | Univers |
|---|---|---|
| `synthese` | tous les KPI en 1 ligne / run (historisable) | univers de chaque ratio |
| `bilan_cas` | LE bilan cas par cas : matchés (par clé — principale / affinée / récupération / **clé clause** —, total, base chute), retrouvés par tentatives (N+1, statut NON **ventilé exercice N / N+1 + total**), non retrouvés de part et d'autre, « à supprimer » encore au compte — nb, PM, taux quand il a un sens, EXPLICATION | tout df_result, un cas = une ligne |
| `taux_chute` | LE taux de chute + composantes PM (base chute, retrouvés, totaux) ; N+1 en regard | base chute (§4.2) |
| `chute_par_exercice` | 1 ligne / exercice : inventaire courant (stats globales), N+1 (analyse séparée) | univers de chute, par exercice |
| `suivi_n1` | consignes des récupérés N+1 (analyse séparée) | `CPT_LATE` hors statut NON |
| `consignes` | conformité + PM + chute par consigne, exercice courant pur | conformité : matchés + missing ; PM/chute : matchés inventaire courant |
| `consignes_par_clause` | tableau de bord : suivi des consignes par TYPE_COMPTE × CLAUSE × CONSIGNE (nb, suivies/non suivies, PM, `NB_NON_REMONTE_DF`) | mêmes règles que `consignes`, ventilées ; repêchés statut NON comptés à part, hors conformité |
| `compte_justification` | décomposition du compte (retrouvés, N+1, repêchés, clos, anomalies) | compte entier |
| `couverture_mrm` | part de la revue retrouvée + non retrouvés par consigne | `MATCHÉS + MRM_MISSING` (+ DELETE retrouvées) |
| `chute_par_clause` | chute par clause × exercice (2 blocs : inventaire courant = stats globales / N+1 = analyse séparée — Σ clauses d'un bloc = taux du bloc, cf. §4.2) | univers de chute, par exercice |
| `chute_par_anciennete` | chute par année de survenance × exercice (N / N-1 / N-2 et antérieur — Σ bloc inventaire = taux principal, cf. §4.2) | univers de chute, par exercice |
| `chute_par_consigne` / `pm_par_consigne` | chute et PM par consigne pertinente (vues de `consignes`) | matchés inventaire courant, KEEP/ADD/STUDY |
| `conformite_consignes` / `conformite_globale` | application des consignes (détail + segments) | exercice courant (§5) |
| `anomalies_cpt_only` | anomalies par mois de survenance (effet fin d'année) | `CPT_ONLY` |
| `orphelins_par_clause` | orphelins compte par clause / compte PB + type, nb + PM + poids + RANG (1 = le plus représentatif, à investiguer avec le souscripteur) — graphe 11 | `CPT_ONLY` |
| `orphelins_par_garantie` | orphelins compte par garantie (IT 60 / IP 64 / autre / non renseignée) | `CPT_ONLY` |
| `orphelins_par_anciennete` | orphelins compte par année de survenance (N / N-1 / N-2 et antérieur) | `CPT_ONLY` |
| `orphelins_cles_nulles` | nullité des colonnes constitutives de la clé (RPP, naissance, survenance, garantie, nom, clause) — explique l'orphelinage | `CPT_ONLY` |
| `controles_coherence` | recoupements inter-tables (attendu / obtenu / OK) : une même grandeur a la même valeur dans tous les onglets Power BI ; bloquant dans le run de production | toutes les tables |

---

## 7. Cohérence d'agrégation (multi-base / multi-inventaire)

- **Intégration N+1** : les `CPT_LATE` sont des dossiers de l'inventaire suivant
  rapatriés ; ils comptent dans la **justification du compte** (bulle
  RETROUVÉS, taux de récupération §3) mais sont **HORS stats globales de
  chute** : leur chute et leur suivi de consignes sont une analyse séparée
  (`chute_n1_*`, `n1_consignes`, cf. §4.2). Aucun calcul global ne doit les
  réintégrer (sinon incohérence chute ↔ consigne).
- **Additivité par clause** : les tables par clause portent
  `(CLAUSE, TYPE_CLAUSE)` ; les % sont calculés **dans le scope de chaque
  clause** (et de chaque exercice pour `chute_par_clause`). La somme des
  numérateurs/dénominateurs par clause = total → on peut agréger plusieurs
  clauses/bases sans recompter.
- **Invariant de cohérence** : `compute_synthese` vérifie que toute ligne tombe
  dans exactement une catégorie connue (`classified_rows == total_rows`) ; sinon
  un `TYPE_RECONCILIATION` inattendu est signalé.
- **Recoupements inter-tables** : `metrics.controles_coherence` vérifie à
  chaque export que les grandeurs partagées se recoupent d'une table à
  l'autre (Σ blocs de `chute_par_clause` / `chute_par_anciennete` == base
  chute / bloc N+1, Σ `anomalies_cpt_only` == CPT_ONLY, Σ des ventilations
  d'orphelins (`orphelins_par_clause` / `_par_garantie` / `_par_anciennete`,
  total de `orphelins_cles_nulles`) == CPT_ONLY, Σ consignes + hors consigne
  == base chute, totaux de `bilan_cas`, etc.) — condition pour que les
  onglets Power BI de l'étude racontent une seule histoire. Table exportée,
  **assert bloquant** dans `notebooks/itip_fiab_powerbi.py`.

---

## 8. Sorties

### 8.1 Formats
- **Excel** (présentation) : un classeur multi-onglets, un onglet par table +
  un onglet « synthèse » lisible (indicateurs cadrés).
- **CSV** / **Parquet** : une table par fichier (rejeu / dataviz).
- **JSON** : une ligne par enregistrement, par table (`export_json`). ✅
- **Base de données** : table Delta metastore (une par analyse).

### 8.2 Ce qui va en base
- Les 20 tables du §6, une table Delta par métrique :
  `<schema>.itip_metric_<nom>_<perim>` (connexion Power BI via SQL Warehouse).
- La table **`synthese`** ✅ tient lieu d'indicateurs scalaires : taux de
  couverture, récupération, chute (+ N+1 séparé), conformité, niveaux de PM,
  volumétries — une ligne par run (date d'inventaire) → historisation et
  suivi dans le temps.

### 8.3 Qualité « présentation »
- Libellés explicites, ordre stable, pourcentages arrondis à 0,1.
- Chaque sortie porte `CLAUSE`, `TYPE_CLAUSE` en tête + date d'inventaire.
- Une page de synthèse condensée (3 indicateurs clés : couverture, récupération,
  chute) directement copiable en slide.

---

## 9. Décisions — tranchées

1. **Taux de chute** = **KEEP + ADD + STUDY** (+ sans consigne reconnue) ;
   DELETE suivi à part (taux de suppression effective). ✅
2. **Univers chute unique = `MATCHÉS` (inventaire courant) hors « à
   supprimer » / statut NON** des deux côtés (taux principal et par
   consigne) ; les récupérés N+1 et les repêchés statut NON sont des
   analyses séparées, hors stats globales (décision du 12/06/2026,
   remplace l'ancien global inv ⊕ N+1). ✅
3. **Synthèse en base** : table `synthese_indicateurs` (1 ligne / run). ✅
4. **JSON** : format ajouté en sortie. ✅
