import pandas as pd
from datetime import datetime
import psycopg2
import requests
from dotenv import load_dotenv
import os

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Récupérer le mot de passe depuis la variable d'environnement

db_host= os.getenv("koyeb_postgres_host")
db_database = os.getenv("koyeb_postgres_db")
db_user = os.getenv("koyeb_postgres_user")
db_password = os.getenv("koyeb_postgres_password")


try:
    conn = psycopg2.connect(
        host=db_host,
        database=db_database,
        user=db_user,
        password=db_password
    )
    cursor = conn.cursor()
except psycopg2.OperationalError as e:
    print(f"Erreur de connexion à la base de données: {e}")
    exit(1)

# Création de la table grippe France
create_grippeRegion_table = '''
CREATE TABLE IF NOT EXISTS grippe_Region (
    week INT,
    indicator INT,
    inc INT,
    inc_low INT,
    inc_up INT,
    inc100 INT,
    inc100_low INT, 
    inc100_up INT,
    geo_insee INT,
    geo_name VARCHAR(255), 
    date DATE
);
'''

# Création de la table grippe France
create_grippeFR_table = '''
CREATE TABLE IF NOT EXISTS grippe_FR (
    week INT,
    indicator INT,
    inc INT,
    inc_low INT,
    inc_up INT,
    inc100 INT,
    inc100_low INT, 
    inc100_up INT,
    geo_insee VARCHAR(255),
    geo_name VARCHAR(255), 
    date DATE

);
'''

# Exécuter les commandes de création des tables
cursor.execute(create_grippeRegion_table)
cursor.execute(create_grippeFR_table)

#Récupération des données via url :

# URL de l'API pour les données d'incidence de la grippe
url_region = "https://www.sentiweb.fr/api/v1/datasets/rest/incidence?indicator=3&geo=RDD&span=all"

# Faire la requête GET pour récupérer les données
response = requests.get(url_region)
# Vérifier si la requête est réussie
if response.status_code == 200:
    # Charger les données JSON
    data = response.json()
    # Extraire les données sous la clé "data"
    data = data.get("data", [])
    # Créer un DataFrame à partir des données
    df = pd.DataFrame(data)
    # Filtrer les données pour novembre 2024, décembre 2024, et janvier 2025
    start_date = 201801
    # Filtrer les données entre les deux dates spécifiées
    df_region = df[(df['week'] >= start_date)]
    # Exporter le DataFrame filtré dans un fichier CSV
    df_region.to_csv(f'incidence_grippe_REGION_{start_date}.csv', index=False)
    print("Les données Région ont été exportées'.")
else:
    print(f"Erreur lors de la récupération des données : {response.status_code}")

# URL de l'API pour les données d'incidence de la grippe
url_Pays = "https://www.sentiweb.fr/api/v1/datasets/rest/incidence?indicator=3&geo=PAY&span=all"
# Faire la requête GET pour récupérer les données
response = requests.get(url_Pays)
# Vérifier si la requête est réussie
if response.status_code == 200:
    # Charger les données JSON
    data = response.json()
    # Extraire les données sous la clé "data"
    data = data.get("data", [])
    # Créer un DataFrame à partir des données
    df = pd.DataFrame(data)
    # Filtrer les données pour novembre 2024, décembre 2024, et janvier 2025
    start_date = 201801
    # Filtrer les données entre les deux dates spécifiées
    df_pays = df[(df['week'] >= start_date)]
    # Exporter le DataFrame filtré dans un fichier CSV
    #df_pays.to_csv(f'incidence_grippe_PAYS_{start_date}.csv', index=False)
    print("Les données France ont été exportées'.")
else:
    print(f"Erreur lors de la récupération des données : {response.status_code}")
    print(response.text) # Afficher le message d'erreur

#Calcul duchap date :
df_region['date'] = df_region['week'].apply(
    lambda x: pd.to_datetime(f'{x // 100}-01-01', format='%Y-%m-%d')
            - pd.DateOffset(days=pd.to_datetime(f'{x // 100}-01-01', format='%Y-%m-%d').weekday())
            + pd.DateOffset(weeks=x % 100 - 1)
            + pd.Timedelta(days=2))

df_pays['date'] = df_pays['week'].apply(
    lambda x: pd.to_datetime(f'{x // 100}-01-01', format='%Y-%m-%d')
            - pd.DateOffset(days=pd.to_datetime(f'{x // 100}-01-01', format='%Y-%m-%d').weekday())
            + pd.DateOffset(weeks=x % 100 - 1)
            + pd.Timedelta(days=2))

# Parcourir chaque ligne du DataFrame
for index, row in df_region.iterrows():
    cursor.execute("""
        INSERT INTO grippe_Region (week, indicator, inc, inc_low, inc_up, inc100, inc100_low, inc100_up, geo_insee, geo_name, date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['week'],
        row['indicator'],
        row['inc'],
        row['inc_low'],
        row['inc_up'],
        row['inc100'],
        row['inc100_low'],
        row['inc100_up'],
        row['geo_insee'],
        row['geo_name'],
        row['date']
    ))

# Parcourir chaque ligne du DataFrame
for index, row in df_pays.iterrows():
    cursor.execute("""
        INSERT INTO grippe_FR (week, indicator, inc, inc_low, inc_up, inc100, inc100_low, inc100_up, geo_insee, geo_name, date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['week'],
        row['indicator'],
        row['inc'],
        row['inc_low'],
        row['inc_up'],
        row['inc100'],
        row['inc100_low'],
        row['inc100_up'],
        row['geo_insee'],
        row['geo_name'],
        row['date']
    ))


# Valider la transaction
conn.commit()

# Fermer la connexion
cursor.close()
conn.close()