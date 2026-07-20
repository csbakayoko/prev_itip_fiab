# Livrables — le point unique des documents du projet ITIP-FIAB

Dernières versions de tous les documents du projet, organisées par
dossier, prêtes pour le dépôt SharePoint. Chaque document existe en éditable +
PDF ; chaque `.md` a son `.html` ; les contenus principaux se déclinent
en niveaux **long → court**. Édition du 19/07/2026 : périmètre
Lab/CORECO clarifié partout, slides finales en fond sombre, diagramme
de Venn dans toutes les versions de deck.

## `presentations/` — la restitution (chiffres réels)

| Fichier | Niveau |
|---|---|
| `Restitution_BackTest_ITIP.pptx` (+ `.pdf`) | **INTÉGRALE — 40 slides** (deck historique de la restitution équipe) |
| `Restitution_BackTest_ITIP_v2.pptx` (+ `.pdf`) | **LONGUE — 27 slides** |
| `Restitution_BackTest_ITIP_v2_MOYENNE.pptx` (+ `.pdf`) | **MOYENNE — 16 slides** |
| `Restitution_BackTest_ITIP_v2_COURTE.pptx` (+ `.pdf`) | **COURTE — 9 slides** |
| `Restitution_Fiabilisation_ITIP.pptx` (+ `.pdf`) | deck phase fiabilisation (juin) — historique |
| `Cartographie_Anomalies_ITIP.pptx` (+ `.pdf`) | support visuel cartographie (10 slides) |
| `Notes_Presentation_BackTest_ITIP.md` (+ `.html`) | notes orales slide par slide |

Toutes les versions v2 incluent le **diagramme de Venn** (« ratios de
mapping ») ; les slides finales (difficultés, comparatif, bilan,
conclusion) sont en **fond sombre** (gabarit `080826`) ; pieds de page
renumérotés par version.

## `documentation/` — documents de référence

| Fichier | Niveau |
|---|---|
| `Documentation_BackTest_ITIP_FIAB_v1.3.docx` (+ `.pdf`) | **LONGUE** — la référence (v1.3 consolidée du 17/07) |
| `Documentation_BackTest_ITIP_SYNTHESE.docx` (+ `.pdf`) | **MOYENNE** — 2 pages |
| `Documentation_BackTest_ITIP_RESUME.docx` (+ `.pdf`) | **COURTE** — 1 page |
| `Rapport_Restitution_BackTest_ITIP_Audit_v1.1.docx` (+ `.pdf`) | rapport fonctions de contrôle (v1.1) |
| `Cartographie_anomalies_orphelins_CPT_MRM.docx` (+ `.pdf`) | cartographie anomalies / orphelins |
| `Trame_entretien_Ali_Ammar_CoreCo_MRM.docx` (+ `.pdf`) | trame d'entretien CoreCo / MRM |
| `RECETTE_ETUDE.docx` (+ `.pdf`) | export de `docs/RECETTE_ETUDE.md` (régénérer, ne pas éditer) |
| `RECETTE_ETUDE.html` · `METRIQUES.html` · `GUIDE_KPI.html` · `POWERBI_MAQUETTE.html` | **versions HTML enrichies** des 4 contrats `docs/*.md` : sommaire navigable, titres émoji, tableaux stylés + illustration d'en-tête (chaîne du pipeline, formule/univers de la chute, cartes KPI fil rouge, modèle en étoile) — régénérer depuis les `.md`, ne pas éditer |

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
(édition du 20/07) : générateur pandas (`genere_jeu_fictif_metriques.py`,
volumes ≈ ÷2, pourcentages décalés, 54 contrôles de cohérence vérifiés),
les 9 tables en CSV (années empilées, clé de liaison `CLE_RUN`) + classeur
`jeu_fictif_metriques.xlsx` prêt pour l'outil de tableau de bord, et son
`LISEZMOI.md` (chiffres de tête + garde-fous). C'est le jeu de la
**maquette du tableau de bord** et des captures école — ⚠ ne pas le
mélanger avec le jeu du deck école ci-dessus ni avec celui du mémoire
(`claude_project_memoire/02_JEU_DONNEES_FICTIF.md`) : un document = un jeu.

## `tutoriels/` — guides techniques (chiffres fictifs)

`Tutoriel_PowerBI_Backtest_ITIP` (docx + pdf + pptx + `_deck.pdf`),
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

`archives/` : versions supplantées (Documentation base/v1.0/v1.1/v1.2,
audit v1.0, decks v2 du 9/07, v3_epuree, v4_ordonnee, backups).
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
