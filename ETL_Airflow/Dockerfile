# Utiliser l'image officielle d'Airflow 2.3.2
FROM apache/airflow:2.3.2

# Passer temporairement en root pour installer les paquets système
USER root
RUN apt-get update && apt-get install -y libpq-dev && rm -rf /var/lib/apt/lists/*

# Revenir à l'utilisateur airflow pour l'installation des packages Python
USER airflow

# Copier et installer les dépendances Python avec l'utilisateur airflow
COPY --chown=airflow:airflow requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt