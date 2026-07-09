# Guide d'interprétation des KPI — ITIP-FIAB (avec exemples chiffrés)

> **But de cette note** : pouvoir **expliquer chaque KPI à l'oral**, en une phrase,
> avec un exemple chiffré. Le contrat formel (formules, univers, limites) reste
> [`METRIQUES.md`](METRIQUES.md) ; ici on ajoute l'**interprétation pédagogique**.
>
> Contexte en une phrase : on réconcilie la **revue MRM** (l'*estimation* d'un
> inventaire : ce qui devrait être provisionné) avec le **compte / CORECO** (le
> *réel* : ce qui est effectivement provisionné), pour répondre à 3 questions —
> **couverture** (la revue est-elle au compte ?), **provisionnement** (les montants
> coïncident-ils ?), **conformité** (les consignes sont-elles appliquées ?).

---

## 0. Le jeu d'exemple « fil rouge »

Tous les exemples ci-dessous utilisent **le même mini-inventaire** (exercice
courant), pour que les chiffres se recoupent d'un KPI à l'autre.

| Catégorie (`TYPE_RECONCILIATION`) | nb | PM MRM | PM compte |
|---|---:|---:|---:|
| Matchés KEEP/ADD/STUDY → **base de chute** | 77 | 1 000 000 € | 900 000 € |
| Matchés DELETE (« à supprimer » encore au compte) | 3 | — | — |
| `MRM_MISSING` (revue non retrouvée au compte) | 20 | 250 000 € | — |
| `MRM_DELETE` absents (suppression suivie, conforme) | 7 | — | — |
| `CPT_LATE` (récupérés dans le MRM N+1) | 5 | 60 000 € | 55 000 € |
| `CPT_ONLY` (anomalies compte) | 15 | — | 120 000 € |

Grandeurs dérivées (à garder en tête) :
- **Matchés inventaire courant** = 77 + 3 = **80**
- **Revue MRM à comparer** = matchés + missing = 80 + 20 = **100**
- **Compte réconciliable** = matchés + N+1 + anomalies = 80 + 5 + 15 = **100**
- **DELETE total** = 3 (encore au compte) + 7 (absents) = **10**

---

## 1. Taux de chute (provisionnement) — *le KPI central*

**Définition.** Écart relatif de provision entre la revue MRM et le compte, sur
les dossiers retrouvés de l'inventaire courant (hors « à supprimer » / statut NON).

**Formule (agrégée).**
```
taux_chute = Σ(PM_MRM − PM_CPT) / Σ(PM_MRM) × 100
```

**Exemple fil rouge.** (1 000 000 − 900 000) / 1 000 000 = **+10 %**.

**À dire à l'oral.** « Sur les dossiers retrouvés, le compte provisionne **10 % de
moins** que l'estimation de la revue : il est **sous-provisionné de 100 000 €**. »

**Sens du signe** (à connaître par cœur) :
- **positif → sous-provisionné** (compte < revue) = **risque** (il manque de la PM) ;
- **négatif → sur-provisionné** (compte > revue) = **marge** (PM en excès).

**Intérêt.** C'est la mesure de **valeur** de l'étude : elle chiffre en euros
l'écart entre ce qui est provisionné et ce qui devrait l'être. C'est ce qui
justifie un ajustement de provision.

**Limites.**
- Mesure une **moyenne pondérée** : un taux de 10 % peut cacher des clauses très
  sous-provisionnées et d'autres sur-provisionnées qui se compensent → toujours
  lire en regard de `chute_par_clause` / `chute_par_consigne`.
- Ne porte que sur les **retrouvés** : un dossier non retrouvé (`MRM_MISSING`) ne
  pèse pas dans la chute (il pèse dans la *couverture*).
- Exclut « à supprimer » (cf. §11) et statut NON (cf. §13, piège 6).

> **Pourquoi une formule AGRÉGÉE et non une moyenne de ratios ?** Soit deux
> dossiers — A : MRM 1 000 000, CPT 950 000 (ratio 5 %) ; B : MRM 1 000, CPT 0
> (ratio 100 %). La **moyenne des ratios** = (5 % + 100 %)/2 = **52,5 %** : aberrant,
> le tout petit dossier B écrase le résultat. L'**agrégé** = (50 000 + 1 000) /
> (1 000 000 + 1 000) = **5,1 %** : correct, chaque euro pèse pareil. On agrège
> donc TOUJOURS Σécarts / ΣPM, jamais une moyenne de pourcentages.

---

## 2. Taux de chute N+1 (récupérés tardifs) — *analyse séparée*

**Définition.** Même formule, mais sur les `CPT_LATE` : orphelins du compte
retrouvés dans l'inventaire **suivant** (N+1).

**Exemple fil rouge.** (60 000 − 55 000) / 60 000 = **+8,3 %**.

**À dire à l'oral.** « Les dossiers rattrapés au N+1 sont eux aussi
sous-provisionnés (8,3 %), mais on les présente **à part** des stats globales. »

**Intérêt.** Montre que les dossiers déclarés en retard suivent la même tendance
(ou non) que l'inventaire courant.

**Limite / cohérence.** **Jamais mélangé au taux principal.** La consigne d'un N+1
vient d'un *autre* inventaire ; l'intégrer fausserait à la fois la chute et la
conformité de la revue courante. D'où deux blocs `EXERCICE` distincts partout.

---

## 3. Taux de chute par consigne / par clause / par ancienneté

**Définition.** Le même taux de chute, **ventilé** par consigne (KEEP/ADD/STUDY),
par clause × exercice, ou par **ancienneté** × exercice (année de survenance
relative à l'inventaire : **N / N-1 / N-2 et antérieur** — bloc `Indéterminée`
si la survenance est nulle ou l'inventaire non daté).

**Exemple fil rouge (consigne « à conserver »).** base 50 matchés, PM MRM
600 000, PM CPT 540 000 → chute = 60 000 / 600 000 = **10 %**.

**À dire à l'oral.** « Le sous-provisionnement vient surtout de la consigne *à
conserver* / de la clause X / des survenances N-1 — c'est là qu'il faut ajuster. »

**Intérêt.** Localise l'écart : **quelles consignes / quelles clauses / quelles
années de survenance portent le risque**. C'est l'angle « action » de l'étude.
Le découpage par ancienneté est **métier** : la méthode d'inventaire diffère
selon l'année (revue tête par tête sur N-1) — un écart concentré sur un bloc
interroge la méthode de ce bloc, pas la revue entière.

**Cohérence garantie.** Dans chaque exercice, **Σ des lignes = le taux global**
(Σécarts / ΣPM par clause redonne `taux_chute_inventaire`). Vérifié à chaque run
par `controles_coherence`.

**Limite.** Une clause à faible PM peut afficher un taux spectaculaire sans enjeu
financier → toujours lire le **poids PM** à côté du taux.

---

## 4. Niveaux de PM (PM MRM, PM compte, écart)

**Définition.** Les **montants bruts** dont le taux de chute est le ratio.

**Exemple fil rouge.** PM MRM 1 000 000 €, PM compte 900 000 €, **écart
+100 000 €** → taux 10 %.

**À dire à l'oral.** « L'écart de provision est de **100 000 €** sur 1 M€ de revue
retrouvée. »

**Intérêt.** Le taux donne l'ampleur *relative*, les niveaux de PM donnent
l'enjeu *absolu* (en euros) — indispensable pour prioriser.

**Limite.** Sur la **base de chute** uniquement (matchés inventaire courant) : ce
n'est pas la PM totale du compte ni de la revue (cf. bulle RETROUVÉS).

---

## 5. Taux de couverture MRM

**Définition.** Part de la revue MRM (à comparer) **retrouvée** au compte, en
nombre de dossiers.

**Formule.** `nb(matchés) / nb(matchés + MRM_MISSING)`.

**Exemple fil rouge.** 80 / (80 + 20) = **80 %**.

**À dire à l'oral.** « **80 %** des dossiers de la revue ont une contrepartie au
compte ; les 20 % restants sont à instruire. »

**Intérêt.** Mesure la **complétude** : la revue est-elle bien reflétée au compte ?

**Limite.** En **nombre**, pas en montant : ne dit rien sur l'écart de PM (c'est le
rôle de la chute). 80 % des dossiers peut représenter 99 % de la PM, ou l'inverse.

---

## 6. Taux de couverture compte

**Définition.** Part du compte réconciliable **justifiée** par un match à
l'inventaire courant.

**Formule.** `nb(matchés) / nb(matchés + CPT_LATE + CPT_ONLY)`.

**Exemple fil rouge.** 80 / (80 + 5 + 15) = **80 %**.

**À dire à l'oral.** « **80 %** des lignes du compte sont justifiées par la revue
courante ; le reste est récupéré au N+1 ou reste en anomalie. »

**Intérêt.** Vision « le compte est-il justifié ? » (miroir de la couverture MRM).

**Limite.** Les récupérés N+1 sont au **dénominateur** mais pas au numérateur (ils
n'ont pas matché l'inventaire *courant*) → ce taux est volontairement *sévère*.

---

## 7. Taux de récupération tardive

**Définition.** Parmi les orphelins du compte après l'inventaire, part rattrapée
dans l'inventaire N+1.

**Formule.** `nb(CPT_LATE) / nb(CPT_LATE + CPT_ONLY)`.

**Exemple fil rouge.** 5 / (5 + 15) = **25 %**.

**À dire à l'oral.** « **1 orphelin sur 4** s'explique par une déclaration tardive
retrouvée au N+1 ; les 75 % restants sont des anomalies. »

**Intérêt.** Distingue le **retard de déclaration** (normal) de la **vraie
anomalie** (à instruire).

**Limite.** Dépend de la fourniture du fichier N+1 (`FICHIER_MRM_N1`). **Sans N+1,
le taux est 0 % — au sens « non mesuré », pas « 0 % de performance ».**

---

## 8. Taux de récupération global

**Définition.** Part du compte réconciliable justifiée par l'inventaire courant
**ou** le N+1.

**Formule.** `nb(matchés + CPT_LATE) / nb(matchés + CPT_LATE + CPT_ONLY)`.

**Exemple fil rouge.** (80 + 5) / 100 = **85 %**.

**À dire à l'oral.** « Au total, **85 %** du compte est justifié ; il reste **15
anomalies** (15 %) sans contrepartie. »

**Intérêt.** Le « solde » de l'exercice : son complément = les anomalies
`CPT_ONLY` définitives, le vrai reste à instruire.

**Limite.** ≠ couverture compte (qui ne compte pas le N+1 au numérateur) — bien
nommer lequel on cite.

---

## 9. Conformité par consigne

**Définition.** La consigne de la revue a-t-elle été appliquée au compte ?

**Règle (le KO est nommé par le FAIT, jamais « non conforme »).**

| Consigne | Retrouvé au compte | Non retrouvé |
|---|---|---|
| à conserver / ajouter / étudier | **conforme** | **non retrouvé** (PM attendue absente) |
| à supprimer | **encore au compte** (KO) | **conforme** (bien supprimé) |

**Exemple fil rouge (« à conserver »).** 50 retrouvées (conformes) + 15 non
retrouvées → conformité = 50 / 65 = **76,9 %**.

**À dire à l'oral.** « **77 %** des dossiers *à conserver* sont bien au compte ; les
autres sont attendus mais absents. »

**Intérêt.** Mesure le **respect des consignes** de la revue, consigne par consigne.

**Limite.** Conformité = **présence / absence**, indépendante du **montant** : un
dossier conforme peut être fortement sous-provisionné (c'est la chute qui le dit).
Les deux lectures (conformité vs provisionnement) sont **complémentaires**, jamais
interchangeables.

**Déclinaison opérationnelle.** La table `consignes_par_clause` applique la même
règle ventilée par **TYPE_COMPTE × CLAUSE × consigne** (nb, suivies / non
suivies, PM) : c'est le « tableau de bord des consignes » Power BI — les cartes
par périmètre (PB / autres) et le tableau filtrable s'en déduisent par simple
filtre. Sa colonne `NB_NON_REMONTE_DF` compte **à part, hors conformité**, les
dossiers repêchés via le statut inventaire NON (PM MRM = 0, non remontée à la
Direction Financière).

---

## 10. Conformité globale

**Définition.** Taux de conformité agrégé sur conserver + ajouter + étudier.

**Formule.** `nb(conformes KAS) / nb(univers KAS)`.

**Exemple fil rouge.** 77 / (77 + 20) = **79,4 %**.

**À dire à l'oral.** « Globalement **79 %** des consignes *à conserver/ajouter/
étudier* sont appliquées. »

**Intérêt.** L'indicateur de **pilotage** unique de la conformité.

**Limite.** Mélange trois consignes de natures différentes → toujours pouvoir
descendre au détail (§9) pour expliquer un écart.

---

## 11. Taux de suppression effective (consigne « à supprimer »)

**Définition.** Part des « à supprimer » réellement **disparues** du compte.

**Formule.** `nb(DELETE absents) / nb(DELETE total)`.

**Exemple fil rouge.** 7 / 10 = **70 %**.

**À dire à l'oral.** « **70 %** des suppressions demandées sont suivies ; **3
dossiers** sont *encore au compte* alors qu'ils devaient disparaître. »

**Intérêt.** Suit une consigne dont l'écart de PM n'a **pas** le sens d'une chute
(la PM devait être supprimée, pas comparée) → on la sort des taux de chute et on
la suit par ce taux dédié.

**Limite.** Les « à supprimer » **conformes** (absentes) n'ont jamais matché → pas
de PM associée ; ce taux est en **nombre**.

---

## 12. Investigation des orphelins compte (`CPT_ONLY`)

**But.** Les anomalies `CPT_ONLY` définitives sont le « reste à instruire » de
l'étude (complément du taux de récupération global, §8). Quatre tables les
découpent pour **orienter l'investigation** — ce sont des vues de diagnostic,
pas des KPI :

| Table | Découpage | Ce qu'elle révèle |
|---|---|---|
| `orphelins_par_clause` | clause (compte PB) × type, avec **RANG** | RANG 1 = le compte le plus représentatif → **à investiguer avec le souscripteur** (graphe 11) |
| `orphelins_par_garantie` | IT (60) / IP (64) / autre / non renseignée | une garantie sur-représentée oriente la recherche de cause |
| `orphelins_par_anciennete` | N / N-1 / N-2 et antérieur | stock ancien (récurrent) vs flux récent (ponctuel) |
| `orphelins_cles_nulles` | % de nullité de chaque composante de la clé (RPP, naissance, survenance, garantie, nom, clause) | une composante souvent vide **explique mécaniquement l'orphelinage** (pas de rapprochement possible) |

**Exemple fil rouge.** Sur les 15 anomalies : le compte PB de RANG 1 en
concentre 8 (53 % du nombre) ; `orphelins_cles_nulles` montre un RPP nul sur
9 dossiers (60 %) → la clé principale ne *pouvait pas* matcher ces lignes.

**À dire à l'oral.** « Les anomalies ne sont pas diffuses : **un compte** en
concentre la moitié, et **le RPP manquant** explique l'essentiel du
non-rapprochement — la question à poser au souscripteur est *comment ces listes
ont été remontées sans apparaître dans la revue*. »

**Intérêt.** Transforme un stock d'anomalies en **plan d'action** ciblé
(quel compte, quelle garantie, quelle donnée manquante).

**Limite.** Les `POIDS_NB_PCT` / `POIDS_PM_PCT` sont des poids **internes aux
orphelins** (Σ = 100 % du bloc `CPT_ONLY`), pas des taux de l'étude. Les quatre
ventilations se recoupent (Σ chacune = total `CPT_ONLY`, vérifié par
`controles_coherence`).

---

## 13. Pièges de lecture (cohérence) — la check-list orale

1. **Nombre vs valeur.** Couverture / conformité = en **dossiers** ; chute /
   niveaux de PM = en **euros**. Ne jamais conclure « bien provisionné » à partir
   d'une couverture élevée.
2. **Signe de la chute.** Positif = **sous**-provisionné (risque) ; négatif =
   **sur**-provisionné (marge). (Couleurs : Sienne = risque, Océan = marge.)
3. **Agrégé, pas moyenne de ratios** (cf. §1) — sinon un micro-dossier fausse tout.
4. **N+1 toujours à part.** Jamais mélangé aux stats globales (chute, conformité) :
   sa consigne vient d'un autre inventaire.
5. **« À supprimer » hors chute.** Un écart sur un dossier à supprimer n'est pas
   une chute → suivi par le *taux de suppression effective*.
6. **Statut NON / obs. tardives IT = hors métriques.** PM MRM = 0 (NON) ou jamais
   matché (obs. tardives) → exclus des taux, présentés à part. Les citer comme
   « explicables », pas comme anomalies.
7. **Couverture compte ≠ récupération globale.** La première ne compte pas le N+1
   au numérateur, la seconde si — citer le bon.

---

## 14. Récapitulatif — une ligne par KPI

| KPI | Formule | Exemple fil rouge | Lecture |
|---|---|---|---|
| Taux de chute | Σ(MRM−CPT)/Σ MRM | +10 % | sous-provisionné de 100 k€ |
| Taux de chute N+1 | idem sur CPT_LATE | +8,3 % | analyse séparée |
| Couverture MRM | matchés / (matchés+missing) | 80 % | revue présente au compte |
| Couverture compte | matchés / réconciliable | 80 % | compte justifié (inventaire) |
| Récup. tardive | LATE / (LATE+CPT_ONLY) | 25 % | orphelins rattrapés au N+1 |
| Récup. globale | (matchés+LATE) / réconciliable | 85 % | compte justifié (inv + N+1) |
| Conformité consigne | conformes / univers consigne | 77 % | consigne appliquée |
| Conformité globale | conformes KAS / univers KAS | 79,4 % | pilotage conformité |
| Suppression effective | DELETE absents / DELETE total | 70 % | suppressions suivies |
