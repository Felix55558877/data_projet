import psycopg2
from psycopg2.extras import execute_batch
import os

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("dbname"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port", 5432),
    )
def upsert_training_data(df):
    """
    Insère ou met à jour les données d'entraînement dans la table season_training_data.
    """
    if df.empty:
        print(" Aucun enregistrement à insérer.")
        return

    cols = list(df.columns)
    rows = [tuple(x) for x in df.to_numpy()]

    placeholders = ", ".join(["%s"] * len(cols))
    columns_str = ", ".join(cols)

    query = f"""
        INSERT INTO season_training_data ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT (match_id) DO UPDATE SET
        {", ".join([f"{col}=EXCLUDED.{col}" for col in cols if col != "match_id"])};
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, query, rows, page_size=100)
        conn.commit()
    print(f"✅ {len(rows)} enregistrements insérés dans season_training_data")