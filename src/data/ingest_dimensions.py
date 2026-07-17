import os
import pandas as pd
import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 1. Conexión a la Base de Datos Local y S3
load_dotenv()
db_url = os.getenv("DATABASE_URL")

if db_url and db_url.startswith("postgresql://"):
    db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)

engine = create_engine(db_url)
s3_client = boto3.client('s3')

bucket_name = 'm5-portfolio-galiana'
print("Conexiones establecidas. Iniciando proceso ELT de dimensiones...")

# --- 2. INGESTA DEL CALENDARIO ---
print("\n--- Procesando Calendario ---")
file_key_cal = '01_raw/calendar.csv'
response_cal = s3_client.get_object(Bucket=bucket_name, Key=file_key_cal)

df_cal = pd.read_csv(response_cal['Body'])
# Forzamos las columnas a minúscula para encajar perfectamente con el SQL (ej. snap_CA -> snap_ca)
df_cal.columns = [col.lower() for col in df_cal.columns]

df_cal.to_sql('stg_calendar', con=engine, if_exists='append', index=False)
print(f"Éxito: {len(df_cal)} filas del calendario inyectadas en tu PC.")

# --- 3. INGESTA DE PRECIOS ---
print("\n--- Procesando Precios ---")
file_key_prices = '01_raw/sell_prices.csv'
response_prices = s3_client.get_object(Bucket=bucket_name, Key=file_key_prices)

# Son unos 6.8 millones de filas, usamos bloques más grandes de 500,000
chunk_iterator = pd.read_csv(response_prices['Body'], chunksize=500000)

for i, chunk in enumerate(chunk_iterator):
    print(f"Inyectando bloque {i + 1} de precios...")
    chunk.to_sql(
        'stg_prices', 
        con=engine, 
        if_exists='append', 
        index=False, 
        method='multi',
        chunksize=10000 
    )

print("\n✅ ¡Misión cumplida! Todos los datos están cargados en PostgreSQL.")