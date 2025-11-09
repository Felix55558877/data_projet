import pandas as pd
from sklearn.preprocessing import LabelEncoder

class DataPreprocessorXGBoost:
    @staticmethod
    def preparer_donnees_xgboost(df):
        colonnes_a_supprimer = ['match_id', 'date_match', 'created_at', 'updated_at']
        colonnes_existantes = [col for col in colonnes_a_supprimer if col in df.columns]
        df_clean = df.drop(columns=colonnes_existantes)
        
        X = df_clean.drop(columns=['result'])
        y = df_clean['result']
        
        le = LabelEncoder()
        y_encoded = le.fit_transform(y)
        
        return X, y_encoded, le

    def filtrer_par_saison(df, saison_id):
        return df[df['season_id'] == saison_id]