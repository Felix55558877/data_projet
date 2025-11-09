#  Football Season Simulation & Prediction

##  Description

Ce projet vise à **prédire les résultats de matchs de football** et à **simuler des saisons complètes** à l’aide d’un modèle de machine learning basé sur **XGBoost**.  
Les données sont extraites, nettoyées et agrégées via un pipeline **ETL**, puis stockées dans une base **Supabase (PostgreSQL)**.

Le modèle s’appuie sur les statistiques des saisons précédentes et les confrontations directes (*head-to-head*) pour estimer la probabilité de victoire, match nul ou défaite pour chaque rencontre.

---

## 🧱 Structure du projet

mon_projet_data/
│
├── anciens_fichier_etl/ # Anciennes versions du pipeline ETL
├── csv_anciennes_versions/ # Sauvegardes CSV précédentes
├── modele_simulation_saison_complete/ # Scripts de simulation Monte Carlo
├── notebooks/ # Analyses exploratoires et tests
├── scripts/
│ ├── etl/ # Chargement des données (extract / transform / load)
│ ├── data_modele_saison.py # Génération du dataset d'entraînement
│ └── xbg_season/ # Entraînement du modèle XGBoost
├── supabase/ # Scripts SQL et configuration de la base
├── .gitignore # Exclusion des fichiers sensibles (ex: myenv, .env)
└── README.md

 Pipeline de données

1. **Extraction :** récupération des données de matchs depuis `match_stats` sur Supabase.  
2. **Transformation :** calcul des statistiques par équipe et des confrontations directes.  
3. **Chargement :** insertion dans la table `training_modele_season`.  
4. **Préparation du modèle :**
   - Variables issues des saisons précédentes (points, buts, possession, etc.)
   - Données de *head-to-head* sur les 5 derniers matchs
   - Encodage des résultats : `home_win`, `draw`, `away_win`

---

##  Modèle de Machine Learning

- **Algorithme :** `XGBoost (multi:softprob)`
- **Cible :** `result` (victoire domicile / nul / victoire extérieur)
- **Évaluation :**
  - Accuracy : ~67–68%
  - Métriques : précision, rappel, F1-score par classe

---

##  Simulation de saison

Une simulation de type **Monte Carlo** permet de :
- prédire tous les matchs d’une saison donnée,
- estimer les classements finaux,
- calculer les probabilités de titre, qualification européenne ou relégation.

##  Exécution

1. **Créer l’environnement virtuel :**
   ```bash
   python -m venv myenv
   source myenv/Scripts/activate

pip install -r requirements.txt
Voir les 2 notebooks pour consulter les details et résultats
notebooks\simulation_Monte_Carlo.ipynb
notebooks\xgboost_football.ipynb