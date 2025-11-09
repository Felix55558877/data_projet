import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
from tqdm import tqdm
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import os

class MonteCarloSimulator:
    def __init__(self):
        self.charger_modele()
        self.resultats_simulations = {}
        
    def charger_modele(self):
        try:
            self.modele = xgb.Booster()
            self.modele.load_model('modele_simulation_saison_complete/modele_xgboost_simulation.json')
            self.metadata = joblib.load('modele_simulation_saison_complete/metadata.pkl')
            self.le = self.metadata['preprocessing']['label_encoder']
            self.features_attendues = self.metadata['preprocessing']['feature_names']
            print(f" Modèle chargé - Accuracy: {self.metadata['performance']['accuracy_test_reference']:.4f}")
        except Exception as e:
            print(f" Erreur chargement modèle: {e}")
            raise
    
    def preparer_calendrier(self, df_saison):
        # S'assurer d'avoir les bonnes colonnes
        colonnes_manquantes = set(self.features_attendues) - set(df_saison.columns)
        if colonnes_manquantes:
            print(f" Colonnes manquantes: {colonnes_manquantes}")
        
        return df_saison[self.features_attendues].copy()
    
    
    def predire_proba_tous_matchs(self, df_calendrier):
        dcal = xgb.DMatrix(df_calendrier)
        raw = self.modele.predict(dcal)  # shape (n, 3) 

        # Construire df_raw avec noms de colonnes du LabelEncoder
        df_raw = pd.DataFrame(raw, columns=self.le.classes_)

        # Mapper vers le point de vue "domicile"
        df = pd.DataFrame({
            'proba_defaite': df_raw['away_win'].astype(float),
            'proba_nul':     df_raw['draw'].astype(float),
            'proba_victoire':df_raw['home_win'].astype(float),
        })

        # clamp + renormalisation par ligne
        df[df < 0] = 0.0
        s = df.sum(axis=1).replace(0, 1.0)
        df = df.div(s, axis=0)

        print(f" Probabilités calculées pour {len(df)} matchs")
        return df
    
    def simuler_un_match(self, probas):
        return np.random.choice(['Défaite', 'Nul', 'Victoire'], p=probas)
    
    def attribuer_points(self, resultat, equipe_domicile, equipe_exterieur, points):
        if resultat == 'Victoire':
            points[equipe_domicile] += 3
        elif resultat == 'Défaite':
            points[equipe_exterieur] += 3
        else:  # Nul
            points[equipe_domicile] += 1
            points[equipe_exterieur] += 1
        return points
    
    def simuler_une_saison(self, df_probas, teams_home=None, teams_away=None):

        from collections import defaultdict
        points = defaultdict(int)
        historique_matchs = []

        # vérifs longueur si on fournit les équipes
        if teams_home is not None and len(teams_home) != len(df_probas):
            raise ValueError("teams_home n'a pas la même longueur que df_probas")
        if teams_away is not None and len(teams_away) != len(df_probas):
            raise ValueError("teams_away n'a pas la même longueur que df_probas")

        for idx, match in df_probas.iterrows():
            equipe_domicile = teams_home[idx] if teams_home is not None else f"Team_Home_{idx}"
            equipe_exterieur = teams_away[idx] if teams_away is not None else f"Team_Away_{idx}"
            probas = [match['proba_defaite'], match['proba_nul'], match['proba_victoire']]

            resultat = self.simuler_un_match(probas)
            points = self.attribuer_points(resultat, equipe_domicile, equipe_exterieur, points)

            historique_matchs.append({
                'home_team': equipe_domicile,
                'away_team': equipe_exterieur,
                'resultat': resultat
            })

        return dict(points), historique_matchs


    def simuler_saison_complete(
        self,
        df_calendrier_features,
        teams_home=None,
        teams_away=None,
        n_simulations=1000,
        seed=None
    ):
       
        import numpy as np
        from collections import defaultdict
        from tqdm import tqdm

        if seed is not None:
            np.random.seed(seed)

        print(f" Lancement de {n_simulations} simulations Monte Carlo...")

        # 1) Probas sur les FEATURES (pas sur predire_proba_tous_matchsdf_saison brut)
        df_probas = self.predire_proba_tous_matchs(df_calendrier_features)
        print(df_probas)
        # 2) Résultats
        tous_classements = []
        points_par_equipe = defaultdict(list)

        # 3) Simulations
        for _ in tqdm(range(n_simulations), desc="Simulations"):
            points_saison, _ = self.simuler_une_saison(df_probas, teams_home, teams_away)
            tous_classements.append(points_saison)
            for equipe, pts in points_saison.items():
                points_par_equipe[equipe].append(pts)

        # 4) Analyse
        analyse = self.analyser_resultats(points_par_equipe)

        self.resultats_simulations = {
            'analyse': analyse,
            'probabilites_matchs': df_probas,
            'points_par_equipe': points_par_equipe,
            'tous_classements': tous_classements
        }
        return analyse, df_probas

    
    def analyser_resultats(self, points_par_equipe):
        """Analyse statistique des résultats des simulations"""
        analyse = {}
        
        for equipe, points_list in points_par_equipe.items():
            points_array = np.array(points_list)
            
            analyse[equipe] = {
                'moyenne_points': float(np.mean(points_array)),
                'mediane_points': float(np.median(points_array)),
                'ecart_type': float(np.std(points_array)),
                'min_points': int(np.min(points_array)),
                'max_points': int(np.max(points_array)),
                'intervalle_confiance_95': [
                    float(np.percentile(points_array, 2.5)),
                    float(np.percentile(points_array, 97.5))
                ]
            }
        
        df_simulations = pd.DataFrame(points_par_equipe)
        analyse['probabilites_classement'] = self.calculer_probabilites_classement(df_simulations)
        
        return analyse
    
    def calculer_probabilites_classement(self, df_simulations):
        n_simulations = len(df_simulations)
        classements = df_simulations.rank(axis=1, ascending=False, method='min')
        
        probabilites = {}
        for equipe in df_simulations.columns:
            pos_counts = classements[equipe].value_counts().sort_index()
            probabilites[equipe] = {pos: count/n_simulations for pos, count in pos_counts.items()}
        
        return probabilites
    
    def simuler_saison_reel(self, df_simulation_reel):
            # Vérifier les colonnes
        required = {'home_team', 'away_team', 'result'}
        missing = required - set(df_simulation_reel.columns)
        if missing:
            raise KeyError(f"Colonnes manquantes: {missing}")

        # Équipes uniques (ordre stable)
        teams = sorted(set(df_simulation_reel['home_team']).union(df_simulation_reel['away_team']))
        points = {str(team): 0 for team in teams}

        # Mapping des labels de résultat attendus par attribuer_points
        map_outcome = {'home_win': 'Victoire', 'away_win': 'Défaite', 'draw': 'Nul'}
        for _, row in df_simulation_reel.iterrows():
            resultat = map_outcome.get(row['result'], row['result'])  # 'Victoire'/'Défaite'/'Nul'
            equipe_domicile = str(row['home_team'])
            equipe_exterieur = str(row['away_team'])
            self.attribuer_points(resultat, equipe_domicile, equipe_exterieur, points)
        
        return points


    def generer_rapport(self):
        if not self.resultats_simulations:
            print(" Aucune simulation effectuée")
            return
        
        analyse = self.resultats_simulations['analyse']
        
        print("\n" + "="*50)
        print(" RAPPORT MONTE CARLO - SIMULATION SAISON")
        print("="*50)
        
        # Classement par moyenne de points
        classement_moyen = sorted(
            [(equipe, stats['moyenne_points']) for equipe, stats in analyse.items() 
             if equipe not in ['probabilites_classement']],
            key=lambda x: x[1], 
            reverse=True
        )
        
        print("\ CLASSEMENT MOYEN (par points moyens):")
        for i, (equipe, points) in enumerate(classement_moyen, 1):
            stats = analyse[equipe]
            print(f"{i:2d}. {equipe:20} {points:5.1f} pts "
                  f"(95% CI: {stats['intervalle_confiance_95'][0]:.1f}-{stats['intervalle_confiance_95'][1]:.1f})")
        
        # Probabilités de titre
        print("\ PROBABILITÉS DE TITRE:")
        probas_titre = {}
        for equipe, probs in analyse['probabilites_classement'].items():
            probas_titre[equipe] = probs.get(1, 0)
        
        for equipe, proba in sorted(probas_titre.items(), key=lambda x: x[1], reverse=True)[:5]:
            if proba > 0:
                print(f"   {equipe:20} {proba:6.2%}")
    
    def visualiser_resultats(self):
        if not self.resultats_simulations:
            return
        
        analyse = self.resultats_simulations['analyse']
        points_par_equipe = self.resultats_simulations['points_par_equipe']
        
        top_equipes = sorted(
            [(equipe, stats['moyenne_points']) for equipe, stats in analyse.items() 
             if equipe not in ['probabilites_classement']],
            key=lambda x: x[1], 
            reverse=True
        )
        
        top_equipes_noms = [equipe for equipe, _ in top_equipes]
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # 1. Distribution des points pour le top 5
        for i, equipe in enumerate(top_equipes_noms[:5]):
            axes[0, 0].hist(points_par_equipe[equipe], bins=20, alpha=0.7, label=equipe)
        axes[0, 0].set_title('Distribution des Points - Top 5 Équipes')
        axes[0, 0].set_xlabel('Points')
        axes[0, 0].set_ylabel('Fréquence')
        axes[0, 0].legend()
        
        # 2. Moyenne de points avec intervalles de confiance
        equipes_visu = top_equipes_noms[:8]
        moyennes = [analyse[eq]['moyenne_points'] for eq in equipes_visu]
        conf_intervals = [analyse[eq]['intervalle_confiance_95'] for eq in equipes_visu]
        errors = [(moy - conf[0], conf[1] - moy) for moy, conf in zip(moyennes, conf_intervals)]
        
        y_pos = np.arange(len(equipes_visu))
        axes[0, 1].barh(y_pos, moyennes, xerr=np.array(errors).T, alpha=0.7)
        axes[0, 1].set_yticks(y_pos)
        axes[0, 1].set_yticklabels(equipes_visu)
        axes[0, 1].set_xlabel('Points Moyens')
        axes[0, 1].set_title('Points Moyens avec Intervalle de Confiance 95%')
        
        # 3. Probabilités de podium
        probas_podium = {}
        for equipe in top_equipes_noms:
            probs = analyse['probabilites_classement'][equipe]
            probas_podium[equipe] = sum(prob for pos, prob in probs.items() if pos <= 3)
        
        axes[1, 0].barh(range(len(probas_podium)), list(probas_podium.values()))
        axes[1, 0].set_yticks(range(len(probas_podium)))
        axes[1, 0].set_yticklabels(list(probas_podium.keys()))
        axes[1, 0].set_xlabel('Probabilité Podium')
        axes[1, 0].set_title('Probabilité de Finir sur le Podium')
        
        # 4. Heatmap des probabilités de classement
        probas_classement = []
        for equipe in top_equipes_noms[:6]:
            probs = analyse['probabilites_classement'][equipe]
            ligne = [probs.get(i, 0) for i in range(1, 7)]
            probas_classement.append(ligne)
        
        im = axes[1, 1].imshow(probas_classement, cmap='YlOrRd', aspect='auto')
        axes[1, 1].set_xticks(range(6))
        axes[1, 1].set_xticklabels(range(1, 7))
        axes[1, 1].set_yticks(range(len(top_equipes_noms[:6])))
        axes[1, 1].set_yticklabels(top_equipes_noms[:6])
        axes[1, 1].set_xlabel('Position Classement')
        axes[1, 1].set_ylabel('Équipe')
        axes[1, 1].set_title('Probabilités de Classement')
        plt.colorbar(im, ax=axes[1, 1])
        
        plt.tight_layout()
        plt.show()
    
    def comparer_reel_modele(self, classement_reel):
        if not self.resultats_simulations:
            raise RuntimeError("Aucune simulation trouvée. Lance simuler_saison_complete() avant.")
        import numpy as np
        import pandas as pd
        from scipy.stats import spearmanr

        points_par_equipe = self.resultats_simulations['points_par_equipe']

        # Points moyens simulés
        points_model_mean = {e: np.mean(pts) for e, pts in points_par_equipe.items()}

        equipes = sorted(set(points_model_mean.keys()) | set(classement_reel.keys()))
        df_comp = pd.DataFrame({
            'equipe': equipes,
            'points_model_mean': [points_model_mean.get(e, np.nan) for e in equipes],
            'points_reels': [classement_reel.get(e, np.nan) for e in equipes],
        })

        # Différences et rangs
        df_comp['diff_points'] = df_comp['points_model_mean'] - df_comp['points_reels']
        df_comp['rank_model'] = df_comp['points_model_mean'].rank(ascending=False, method='min').astype(int)
        df_comp['rank_reel']  = df_comp['points_reels'].rank(ascending=False, method='min').astype(int)
        df_comp['diff_rank']  = df_comp['rank_model'] - df_comp['rank_reel']
        df_comp = df_comp.sort_values('rank_reel').reset_index(drop=True)

        # Métriques globales
        mae_points = df_comp['diff_points'].abs().mean()
        rmse_points = np.sqrt((df_comp['diff_points']**2).mean())
        spearman_r = spearmanr(df_comp['rank_model'], df_comp['rank_reel']).correlation

        top4_model = set(df_comp.sort_values('rank_model').head(4)['equipe'])
        top4_reel  = set(df_comp.sort_values('rank_reel').head(4)['equipe'])
        top4_overlap = len(top4_model & top4_reel)

        champion_model = df_comp.loc[df_comp['rank_model'].idxmin(), 'equipe']
        champion_reel  = df_comp.loc[df_comp['rank_reel'].idxmin(), 'equipe']

        metrics = {
            'MAE_points': round(mae_points, 2),
            'RMSE_points': round(rmse_points, 2),
            'Spearman_ranks': round(spearman_r, 3),
            'Top4_overlap': f"{top4_overlap}/4",
            'Champion_model': champion_model,
            'Champion_reel': champion_reel,
            'Champion_match': champion_model == champion_reel
        }

        self.comparaison_resultats = {'df_comp': df_comp, 'metrics': metrics}
        return df_comp, metrics
    
    def visualiser_comparaisons(self):
        """Affiche les visuels comparatifs modèle vs réel."""
        if not hasattr(self, 'comparaison_resultats'):
            raise RuntimeError("Lance comparer_reel_modele() avant d’appeler cette fonction.")

        import matplotlib.pyplot as plt
        import numpy as np

        df_comp = self.comparaison_resultats['df_comp']

        # Barres : points réels vs simulés
        df_plot = df_comp.sort_values('points_reels', ascending=False)
        x = np.arange(len(df_plot)); w = 0.4

        plt.figure(figsize=(12,6))
        plt.bar(x - w/2, df_plot['points_reels'], width=w, label='Réel')
        plt.bar(x + w/2, df_plot['points_model_mean'], width=w, label='Modèle (moyenne)')
        plt.xticks(x, df_plot['equipe'], rotation=60, ha='right')
        plt.ylabel('Points')
        plt.title('Points réels vs simulés (moyenne)')
        plt.legend()
        plt.tight_layout()
        plt.show()

        #  Scatter : corrél
        plt.figure(figsize=(6,6))
        plt.scatter(df_comp['points_reels'], df_comp['points_model_mean'])
        m = float(np.nanmax(df_comp[['points_reels','points_model_mean']].values)) + 5
        plt.plot([0,m],[0,m])
        plt.xlabel('Points réels')
        plt.ylabel('Points simulés (moyenne)')
        plt.title('Corrélation points — réel vs modèle')
        plt.tight_layout()
        plt.show()

        # Écarts de rang 
        df_rank = df_comp.sort_values('diff_rank')
        y = np.arange(len(df_rank))
        plt.figure(figsize=(10,10))
        plt.barh(y, df_rank['diff_rank'])
        plt.yticks(y, df_rank['equipe'])
        plt.xlabel('Différence de rang (modèle réel)')
        plt.title('Écart de rang par équipe')
        plt.tight_layout()
        plt.show()