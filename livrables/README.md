# Livrables — le point unique des documents du projet ITIP-FIAB

Dernières versions de tous les documents du projet, organisées par
dossier, prêtes pour le dépôt SharePoint. Chaque document existe en éditable +
PDF ; chaque `.md` a son `.html` ; les contenus principaux se déclinent
en niveaux **long → court**.

**Édition du 10/09/2026** — dépôt de passation complété et remis à niveau :
- nouveau dossier `09 - Mémoire et soutenance` (mémoire d'actuariat, note
  de synthèse, résumé FR/EN, support de soutenance) et guide du dépôt mis
  à jour en conséquence (plan, orientation par question, confidentialité) ;
- `07 - Code du traitement` : archive de livraison v2.0 + notes de version ;
- **exports des contrats régénérés** — les HTML et les Word/PDF de `04` et
  `05` dataient d'avant le passage à 10 tables : ils annonçaient encore
  « 8 tables » et ignoraient la table `distribution_ecarts`. Ils sont
  reconstruits depuis les sources `docs/*.md`, liens internes compris ;
- **jeu d'essai fictif remis au schéma courant** — il lui manquait la table
  `distribution_ecarts` (la page P3 de la maquette n'avait donc pas de
  source) et sa table `chute` portait encore l'axe « Tranche d'écart »,
  supprimé en production. 10 tables désormais, 63 contrôles au vert ;
- **documentation de référence en v1.4**, rapport d'audit en v1.2 : annexe
  des tables complétée de `dim_run` et `distribution_ecarts`, comptes
  portés à 10 tables / 12 graphiques (les v1.3 et v1.1 sont en `archives/`) ;
- **tutoriel du rapport entièrement refondu** (Word, PDF et support) — il
  enseignait encore trois tables de dimension construites à la main dans
  Power Query, alors que `metrique_dim_run` et la clé `CLE_RUN` les
  remplacent. Désormais : 11 tables à importer, une seule dimension et dix
  relations, tri des axes par `ORDRE`, et les pages vont jusqu'à **P8**
  (nouvelle page P3 « Écarts par dossier », dans le document comme dans le
  support, qui passe de 16 à 17 diapositives).

Édition du 22/07/2026 : cartographie des anomalies portée en **v1.2**
(éclairage à la source issu de l'échange Ali Ammar, nouvelle anomalie A14
« date de naissance chargée dans la date de survenance », causes
confirmées sur A04/A08/A09-A10/A11) + nouveaux livrables de synthèse de
cet échange (Word + support de présentation).

## `Depot_SharePoint_Backtest_ITIP/` — le dépôt de passation

L'arborescence prête à déposer telle quelle, organisée par usage. Chaque
dossier « matière » porte un `A_PROPOS.txt` qui rappelle ce qu'on y range
et les conventions de nommage ; `00 - Guide du dépôt` donne le plan et la
table « une question → le bon document ».

| Dossier | Contenu |
|---|---|
| `00 - Guide du dépôt` | Le guide (Word + PDF) — plan du dépôt, orientation par question, conventions, confidentialité |
| `01 - Présentations` | Supports de restitution (intégrale → longue → moyenne → courte), cartographie, synthèse de l'échange CoreCo, notes orales |
| `02 - Documentation de référence` | Documentation v1.4 et ses déclinaisons, rapport pour les fonctions de contrôle |
| `03 - Qualité des données` | Cartographie des anomalies v1.2 (A01→A14), synthèse de l'échange, trame d'entretien |
| `04 - Méthode et indicateurs` | Les 4 contrats (métriques, guide des indicateurs, recette de l'étude, maquette du rapport) + le tutoriel du traitement, en HTML — plus la recette en Word/PDF |
| `05 - Tutoriels` | Relance de la production semestrielle, construction du rapport, jeu d'essai fictif |
| `06 - Pièces jointes` | Pièces reçues des interlocuteurs + images et captures — à ne pas modifier |
| `07 - Code du traitement` | Archives de livraison du code + notes de version (à alimenter au moment du dépôt) |
| `08 - Power BI` | Fichier du rapport, thème, captures des pages, notes de connexion (à alimenter) |
| `09 - Mémoire et soutenance` | Le mémoire d'actuariat, la note de synthèse, le résumé (FR/EN) et le support de soutenance — **chiffres entièrement fictifs, diffusables hors équipe** |

⚠ Le dossier `09` est le seul, avec le jeu d'essai du `05`, à pouvoir
sortir de l'équipe. En sens inverse, aucun de ses chiffres ne doit
remonter dans les dossiers `01` à `04` : eux seuls portent les chiffres
réels.

**Le dépôt fait foi.** Les dossiers à plat ci-dessous (`presentations/`,
`documentation/`, `tutoriels/`, `ecole/`) sont l'atelier : c'est là qu'on
fabrique et qu'on régénère. Le dépôt en est le miroir publié. Toute
nouvelle version se recopie donc dans les deux — une correction faite d'un
seul côté crée exactement la dérive qui a été rattrapée le 10/09 (des
exports en retard d'un refactor annonçaient encore « 8 tables »).

## `presentations/` — la restitution (chiffres réels)

| Fichier | Niveau |
|---|---|
| `Restitution_BackTest_ITIP.pptx` (+ `.pdf`) | **INTÉGRALE — 40 slides** (deck historique de la restitution équipe) |
| `Restitution_BackTest_ITIP_v2.pptx` (+ `.pdf`) | **LONGUE — 27 slides** |
| `Restitution_BackTest_ITIP_v2_MOYENNE.pptx` (+ `.pdf`) | **MOYENNE — 16 slides** |
| `Restitution_BackTest_ITIP_v2_COURTE.pptx` (+ `.pdf`) | **COURTE — 9 slides** |
| `Restitution_Fiabilisation_ITIP.pptx` (+ `.pdf`) | deck phase fiabilisation (juin) — historique |
| `Cartographie_Anomalies_ITIP.pptx` (+ `.pdf`) | support visuel cartographie (10 slides) |
| `Synthese_Echange_Ali_Ammar_Champs_Obligatoires.pptx` (+ `.pdf`) | synthèse de l'échange Ali Ammar — alimentation CORECO → Lab & champs obligatoires (7 slides, restitution du 23/07) |
| `Notes_Presentation_BackTest_ITIP.md` (+ `.html`) | notes orales slide par slide |

Toutes les versions v2 incluent le **diagramme de Venn** (« ratios de
mapping ») ; les slides finales (difficultés, comparatif, bilan,
conclusion) sont en **fond sombre** (gabarit `080826`) ; pieds de page
renumérotés par version.

## `documentation/` — documents de référence

| Fichier | Niveau |
|---|---|
| `Documentation_BackTest_ITIP_FIAB_v1.4.docx` (+ `.pdf`) | **LONGUE** — la référence (v1.4 du 10/09 : annexe C complétée de `dim_run` et `distribution_ecarts`, comptes portés à 10 tables / 12 graphiques ; v1.3 en `archives/`) |
| `Documentation_BackTest_ITIP_SYNTHESE.docx` (+ `.pdf`) | **MOYENNE** — 2 pages |
| `Documentation_BackTest_ITIP_RESUME.docx` (+ `.pdf`) | **COURTE** — 1 page |
| `Rapport_Restitution_BackTest_ITIP_Audit_v1.2.docx` (+ `.pdf`) | rapport fonctions de contrôle (v1.2 du 10/09 : 10 tables ; v1.1 en `archives/`) |
| `Cartographie_anomalies_orphelins_CPT_MRM.docx` (+ `.pdf`) | cartographie anomalies / orphelins — **v1.2 du 22/07** (13 fiches A01→A14, section « éclairage à la source » ; fiche « dossiers non retrouvés » retirée, code A07 non réattribué ; v1.1 en `archives/`) |
| `Trame_entretien_Ali_Ammar_CoreCo_MRM.docx` (+ `.pdf`) | trame d'entretien CoreCo / MRM |
| `Synthese_Echange_Ali_Ammar_Champs_Obligatoires.docx` (+ `.pdf`) | synthèse de l'échange Ali Ammar : alimentation CORECO → Lab, champs obligatoires/facultatifs, confrontation aux constats, prochaines étapes |
| `Mails_Investigation_Clauses_CC2023.md` | trames des messages **individuels** aux préparateurs de comptes (une clause = un analyste = un message) : contexte du contrôle, mécanique du rapprochement, 6 questions, variante deux clauses, points de vigilance (données nominatives) — pièces jointes produites par le notebook `itip_fiab_extraction_anomalies` |
| `RECETTE_ETUDE.docx` (+ `.pdf`) | export de `docs/RECETTE_ETUDE.md` (régénérer, ne pas éditer) |
| `RECETTE_ETUDE.html` · `METRIQUES.html` · `GUIDE_KPI.html` · `POWERBI_MAQUETTE.html` · `TUTORIEL_JOB_DATABRICKS.html` | versions HTML des contrats `docs/*.md` : page autonome, titre émoji, tableaux stylés, liens entre documents résolus vers les `.html` — **régénérées le 10/09 depuis les sources, ne pas éditer** (`pandoc -s fichier.md -o fichier.html --metadata title="…"`) |

## `ecole/` — version académique ANONYMISÉE

`Restitution_BackTest_ITIP_ECOLE.pptx` (+ `.pdf`) — le deck LONGUE (27
slides) avec **jeu de données fictif cohérent** : noms d'exemple
anonymisés (DUPONT-MARTIN CHRISTELLE, la troncature à 20 caractères
reste démontrable), volumétries, PM et taux recalculés pour que toutes
les identités comptables tiennent (union 13 910 = 12 480 + 6 350 −
4 920 ; cascade 4 610+245+32+9+2+22 = 4 920 ; orphelins 1 430 =
590+275+155+410 ; chute −2,2 % = −5,8/262,7 ; justification 93,5 % ;
2024 : 17 240 lignes, −1,2 %, 82,5 %, 1 260 anomalies). Mention
« DONNÉES ANONYMISÉES » sur la page de titre. Audit anti-fuite passé :
aucun chiffre réel résiduel. **Seule version à utiliser hors AXA**
(mémoire / soutenance ISFA).

`jeu_fictif_metriques/` — **le jeu de MÉTRIQUES fictives 2023 + 2024**
(édition du 20/07, complété le 10/09) : générateur pandas
(`genere_jeu_fictif_metriques.py`, volumes ≈ ÷2, pourcentages décalés,
63 contrôles de cohérence vérifiés), les 10 tables en CSV (années empilées, clé de liaison `CLE_RUN`) + classeur
`jeu_fictif_metriques.xlsx` prêt pour l'outil de tableau de bord, et son
`LISEZMOI.md` (chiffres de tête + garde-fous). C'est le jeu de la
**maquette du tableau de bord** et des captures école — ⚠ ne pas le
mélanger avec le jeu du deck école ci-dessus ni avec celui du mémoire
(`claude_project_memoire/02_JEU_DONNEES_FICTIF.md`) : un document = un jeu.

## `tutoriels/` — guides techniques (chiffres fictifs)

`Tutoriel_PowerBI_Backtest_ITIP` (docx + pdf + pptx + `_deck.pdf` — **refondu
le 10/09** : 11 tables, modèle en étoile par `CLE_RUN`, tri par `ORDRE`,
pages P1→P8 dont la nouvelle P3 « Écarts par dossier » ; 17 diapositives),
`Tutoriel_Job_Databricks_ITIP` (docx + pdf — la source à jour est
`docs/TUTORIEL_JOB_DATABRICKS.md`, régénérer les exports depuis elle),
`PROMPT_CLAUDE_DESIGN.md` (+ `.html`) — **réécrit le 20/07** : prompt
complet de la maquette DYNAMIQUE du rapport (un fichier HTML autonome,
visuels 100 % natifs Power BI, 8 onglets + page détail, jeu
`jeu_fictif_metriques` embarqué ; le `.html` d'ancienne génération est à
régénérer), `PROMPTS_SCHEMAS.md` (+ `.html`).

## `claude_project/` — docs de connaissance pour projet Claude (travail)

`01_PROJET_COMPLET.md` (long) · `02_SYNTHESE.md` (moyen) ·
`03_RESUME.md` (court) · `00_INDEX.md` (mode d'emploi) — chacun avec
son `.html`. **Chiffres réels** — usage interne AXA uniquement.

## `claude_project_memoire/` — kit du projet Claude bi-usage (mémoire + entreprise)

Kit complet d'un projet Claude à **deux modes cloisonnés** :
🎓 mémoire/soutenance (jeu fictif `02_JEU_DONNEES_FICTIF` — seul jeu
autorisé dans le mémoire) et 🏢 livrables d'entreprise (référentiel réel
`05_REFERENTIEL_ENTREPRISE_REEL` + conventions/gabarits
`06_LIVRABLES_ENTREPRISE`). Instructions du projet dans
`00_INSTRUCTIONS_PROJET.md`, contexte `01`, plan de travail mémoire
`03`, prompts de figures `04`, mode d'emploi + checklist d'upload dans
son `README.md`.

## `archives/` et `photos/`

`archives/` : versions supplantées (Documentation base/v1.0/v1.1/v1.2/v1.3,
audit v1.0/v1.1, cartographie des anomalies v1.1, decks v2 du 9/07,
v3_epuree, v4_ordonnee, backups).
`photos/` : images IMG_* rapatriées de Downloads, à trier.

## Règles

- **La LONGUE fait foi** ; les niveaux courts et les exports en dérivent.
- Chiffres **réels** = `presentations/` + `documentation/` (diffusion
  interne) ; chiffres **fictifs** = `tutoriels/` + `ecole/`. Ne jamais
  mélanger ; le mémoire ISFA n'utilise que `ecole/`.
- Formulation de périmètre : « le Lab Databricks s'élargit aux autres
  périmètres (déjà sous CORECO) » — jamais « le portefeuille bascule
  sous CORECO ».
- Régénération PDF : `soffice --headless --convert-to pdf <fichier>` ;
  HTML : `pandoc -s fichier.md -o fichier.html`.
