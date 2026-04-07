from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'amazon_recommender_pipeline',
    default_args=default_args,
    description='Pipeline de recommandation Amazon: Entraînement ALS & Inférence Streaming',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['spark', 'machine-learning', 'streaming'],
) as dag:

    # Tâche 1 : Entraînement du modèle
    train_model = BashOperator(
        task_id='train_model',
        bash_command='docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/src/spark/train_model.py',
    )

    # Tâche 2 : Démarrage du streaming avec les drivers
    start_streaming = BashOperator(
        task_id='start_streaming',
        bash_command='docker exec -d spark_master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 /opt/spark/src/spark/streaming_recommender.py',
    )

    train_model >> start_streaming