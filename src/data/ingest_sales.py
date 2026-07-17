import os
import pandas as pd
import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 1. Cargar secretos del archivo .env
load_dotenv()
db_url = os.getenv("DATABASE_URL") #database url

# SQLAlchemy requiere un pequeño ajuste en el prefijo de la URL de Neon
if db_url and db_url.startswith("postgresql://"):
    db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1) #SQLAlchemy necesita saber el driver que usaremos: psycopg2, que es el traductor estándar de PostgreSQL para Python

# 2. Conectar a PostgreSQL y AWS S3
engine = create_engine(db_url) #engine que solo conecta a Neon cuando tiene datos listos para enviar
s3_client = boto3.client('s3') #Python lee automáticamente las claves de tu usuario IAM que pusiste en el .env y se autentica en la nube

bucket_name = 'm5-portfolio-galiana'
file_key = '01_raw/sales_train_validation.csv'

print("Conectando al Data Lake en S3...")
response = s3_client.get_object(Bucket=bucket_name, Key=file_key) #No descarga el archivo a tu disco duro, es un stream directo entre los servidores de Amazon y mi memoria RAM

# 3. Configurar el procesamiento por Chunks
# Procesamos solo 100 filas originales a la vez. 
# Al pivotar (100 filas x 1913 días), enviaremos paquetes de ~190,000 registros a la BD.
chunk_size = 100
id_columns = ['id', 'item_id', 'dept_id', 'cat_id', 'store_id', 'state_id']

print("Iniciando flujo de datos (Stream) y transformación (Unpivot)...")
chunk_iterator = pd.read_csv(response['Body'], chunksize=chunk_size) #al añadir parámetro chunksize, en lugar de un DataFrame devuelve un TextFileReader

for i, chunk in enumerate(chunk_iterator):
    print(f"Procesando bloque {i + 1}...")
    
    # Transformación: Formato Ancho (Wide) a Formato Largo (Long)
    chunk_melted = pd.melt( #.melt mira las columnas que NO hayas puesto en id_vars y asume automáticamente que quieres pivotarla hacia abajo
        chunk, 
        id_vars=id_columns, #seleccionar las columnas que NO se van a pivotar
        var_name='d', #nombre de la columna que contendrá los nombres de las columnas pivotadas (d_1, d_2, ..., d_1913)
        value_name='sales_qty' #nombre de la columna que contendrá los valores de las columnas pivotadas (las ventas diarias)
    )
    
    # Carga: Insertar en la tabla stg_sales de Neon
    chunk_melted.to_sql( #utiliza el engine para enviar datos de RAM a Neon
        'stg_sales', #nombre de la tabla de destino
        con=engine, #conexión a la base de datos
        if_exists='append', #si la tabla ya existe, añadir datos debajo de los anteriores
        index=False, #no incluir el índice del DataFrame
        method='multi', #empaqueta múltiples registros en una sola consulta para minimizar latencia de red
        chunksize=10000  #tamaño del lote
    )
    
    print(f"Bloque {i + 1} cargado en PostgreSQL con éxito.")

print("Operación ELT finalizada al 100%.")