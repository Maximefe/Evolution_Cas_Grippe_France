from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import datetime, timedelta
import requests
import pandas as pd
import io
import os
from sqlalchemy import create_engine

# 📌 Configuration
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

API_URLS = {
    "vaccins2024": "https://www.data.gouv.fr/fr/datasets/r/848e3e48-4971-4dc5-97c7-d856cdfde2f6",
    "vaccins2023": "https://www.data.gouv.fr/fr/datasets/r/1b5339fe-47b9-4d29-9be6-792ac20e392b"
}

def get_file_path(data_type, stage="raw"):
    """Génère le nom de fichier avec un timestamp et un stage (raw, transformed)"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(DATA_DIR, f"{stage}_{data_type}_{timestamp}.csv")


def extract_data(data_type, **context):
    """Extrait les données d'une API et les stocke en CSV"""
    try:
        url = API_URLS.get(data_type)
        if not url:
            raise ValueError(f"Type de données inconnu: {data_type}")
        
        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text))
        file_path = get_file_path(data_type, "raw")
        df.to_csv(file_path, index=False)
        
        context['task_instance'].xcom_push(key=f'file_path_{data_type}', value=file_path)
        LoggingMixin().log.info(f"✅ {data_type.upper()} - Fichier RAW sauvegardé : {file_path}")
    
    except Exception as e:
        LoggingMixin().log.error(f"❌ Erreur lors de l'extraction des données {data_type} : {e}")
        raise Exception(f"❌ Erreur lors de l'extraction des données {data_type} : {e}")

def transform_data(data_type, **context):
    """Transforme les données: suppression des doublons et des valeurs vides"""
    try:
        ti = context['task_instance']
        raw_file_path = ti.xcom_pull(task_ids=f'extract_{data_type}', key=f'file_path_{data_type}')
        
        if not raw_file_path or not os.path.exists(raw_file_path):
            raise Exception(f"❌ Fichier RAW {data_type} introuvable : {raw_file_path}")

        df = pd.read_csv(raw_file_path)
        
        # Suppression des valeurs manquantes et doublons
        df_cleaned = df.dropna().drop_duplicates()
        transformed_file_path = get_file_path(data_type, "transformed")
        df_cleaned.to_csv(transformed_file_path, index=False)
        
        context['task_instance'].xcom_push(key=f'transformed_file_path_{data_type}', value=transformed_file_path)
        print(f"✅ {data_type.upper()} - Fichier TRANSFORMÉ sauvegardé : {transformed_file_path} ({len(df_cleaned)} lignes)")

    except Exception as e:
        raise Exception(f"❌ Erreur lors de la transformation des données {data_type} : {e}")

def load_data_to_koyeb(data_type, table_name, **context):
    """Charge un fichier transformé dans PostgreSQL"""
    try:
        # Connexion PostgreSQL
        host = Variable.get("koyeb_postgres_host")
        login = Variable.get("koyeb_postgres_user")
        password = Variable.get("koyeb_postgres_password")
        port = Variable.get("koyeb_postgres_port")
        db = Variable.get("koyeb_postgres_db")

        endpoint_id = host.split('.')[0]
        connection_string = f"postgresql://{login}:{password}@{host}:{port}/{db}?options=endpoint%3D{endpoint_id}&sslmode=require"
        engine = create_engine(connection_string)

        # Récupération du fichier transformé depuis XCom
        ti = context['task_instance']
        transformed_file_path = ti.xcom_pull(task_ids=f'transform_{data_type}', key=f'transformed_file_path_{data_type}')
        
        if not transformed_file_path or not os.path.exists(transformed_file_path):
            raise Exception(f"❌ Fichier TRANSFORMÉ {data_type} introuvable : {transformed_file_path}")

        df = pd.read_csv(transformed_file_path)

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )

        print(f"✅ Chargement réussi dans la table {table_name} ({len(df)} lignes)")

    except Exception as e:
        raise Exception(f"❌ Erreur lors du chargement des données {data_type} : {e}")

# 📌 Configuration du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'grippe_ETL_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 2, 4, 9, 0, 0),
    schedule_interval='@hourly',
    max_active_runs=1,
    catchup=False
) as dag:

    # 📌 Extraction des données
    extract_vaccins2023_task = PythonOperator(
        task_id='extract_vaccins2023',
        python_callable=extract_data,
        op_kwargs={'data_type': 'vaccins2023'},
        provide_context=True
    )

    extract_vaccins2024_task = PythonOperator(
        task_id='extract_vaccins2024',
        python_callable=extract_data,
        op_kwargs={'data_type': 'vaccins2024'},
        provide_context=True
    )

    # 📌 Transformation des données
    transform_vaccins2023_task = PythonOperator(
        task_id='transform_vaccins2023',
        python_callable=transform_data,
        op_kwargs={'data_type': 'vaccins2023'},
        provide_context=True
    )

    transform_vaccins2024_task = PythonOperator(
        task_id='transform_vaccins2024',
        python_callable=transform_data,
        op_kwargs={'data_type': 'vaccins2024'},
        provide_context=True
    )

    # 📌 Chargement des données en base
    load_vaccins2023_task = PythonOperator(
        task_id='load_vaccins2023',
        python_callable=load_data_to_koyeb,
        op_kwargs={'data_type': 'vaccins2023', 'table_name': 'grippe_vaccins2023'},
        provide_context=True
    )

    load_vaccins2024_task = PythonOperator(
        task_id='load_vaccins2024',
        python_callable=load_data_to_koyeb,
        op_kwargs={'data_type': 'vaccins2024', 'table_name': 'grippe_vaccins2024'},
        provide_context=True
    )
    # 🔗 Définition du workflow : EXTRACT -> TRANSFORM -> LOAD

    # Vaccins 2023
    extract_vaccins2023_task >> transform_vaccins2023_task >> load_vaccins2023_task

    # Vaccins 2024
    extract_vaccins2024_task >> transform_vaccins2024_task >> load_vaccins2024_task