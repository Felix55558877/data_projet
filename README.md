# Résumé projet Data : ETL, automatisation, config & structure Git

## 1. Problématique
- Trouver une problématique métier ou scientifique.
- Collecter les données (extraction).
- Construire un pipeline ETL (Extract, Transform, Load).
- Définir un schéma de données SQL pour stocker proprement les données.
- Développer un modèle prédictif.
- Automatiser l’extraction (et idéalement tout l’ETL) avec un déclenchement régulier (ex : quotidien).
- Versionner et collaborer via un dépôt Git.

## 2. Pipeline ETL : focus sur l’extraction

### a. Extraction
- Objectif : récupérer les données brutes depuis une source (API, fichier en ligne, base externe).
- Script dédié : `src/data_engineering/extract.py`
- Récupère les données automatiquement (ex : via API, requête HTTP, streaming).
- Sauvegarde les données brutes dans un dossier `data/raw/`.
- Paramètres (URL, clé API, chemins) externalisés dans `config.yaml`.

### b. Transformation
- Nettoyage, mise en forme, normalisation.
- Script : `src/data_engineering/transform.py`
- Lit les données brutes, produit des données nettoyées/transformées dans `data/processed/`.

### c. Chargement
- Chargement dans la base de données SQL.
- Script : `src/data_engineering/load.py`
- Utilise le schéma SQL défini, se connecte à la base avec paramètres du fichier config.

## 3. Automatisation de l’extraction (et ETL)
- Script central : `scripts/run_etl.py`
- Appelle `extract.py`, `transform.py`, `load.py` dans cet ordre.
- Automatisation via GitHub Actions (exemple dans `.github/workflows/extract.yml`) :  
  déclenche `run_etl.py` automatiquement tous les jours (ou à la fréquence souhaitée).
- Avantages :  
  - Pas besoin de garder ta machine allumée.  
  - Exécution fiable et traçable.  
  - Déploiement simple.

## 4. Fichier de configuration `config.yaml`
- Centralise tous les paramètres importants :  
  - URL des sources de données et clés API.  
  - Chemins de stockage des données brutes et transformées.  
  - Paramètres de connexion à la base SQL.  
  - Autres seuils ou options.
- Permet d’adapter facilement le projet à différents environnements sans modifier le code.
- Chargé au début des scripts (ex : `extract.py`, `load.py`) pour lire les paramètres.

## 5. Structure détaillée du repo Git

mon_projet_data/
│
├── src/
│ └── data_engineering/
│ ├── extract.py # Extraction des données (source API/fichiers)
│ ├── transform.py # Nettoyage & transformation des données
│ └── load.py # Chargement dans base SQL
│
├── data/
│ ├── raw/ # Données brutes extraites (ex : JSON, CSV)
│ └── processed/ # Données transformées prêtes à charger
│
├── models/ # Modèles prédictifs & scripts entraînement
│ └── train_model.py
│
├── notebooks/ # Exploration, analyse et visualisation
│
├── scripts/ # Scripts d’exécution (ETL complet)
│ └── run_etl.py # Script qui lance extract + transform + load
│
├── .github/ # Automatisation GitHub Actions
│ └── workflows/
│ └── extract.yml # Workflow GitHub pour lancer run_etl.py régulièrement
│
├── config.yaml # Fichier de configuration central (URL, API key, chemins, DB)
├── requirements.txt # Dépendances Python
└── README.md # Documentation du projet


## 6. Exemple de fonctionnement résumé
- Le workflow GitHub Actions déclenche `scripts/run_etl.py` automatiquement tous les jours.
- `run_etl.py` importe et exécute dans l’ordre :  
  - `extract.py` : récupère les données brutes via API (params dans `config.yaml`), stocke dans `data/raw/`.  
  - `transform.py` : lit les données brutes, nettoie et prépare les données, sauvegarde dans `data/processed/`.  
  - `load.py` : charge les données nettoyées dans une base SQL (connexion via `config.yaml`).
- Tu peux ensuite utiliser `models/train_model.py` pour entraîner un modèle prédictif sur les données transformées.
- Tout le code est versionné sur GitHub, facilitant la collaboration.
