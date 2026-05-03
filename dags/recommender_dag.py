from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2, # Un peu plus de résilience
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'amazon_recommender_pipeline',
    default_args=default_args,
    description='Pipeline Parallèle: Ingestion Temps Réel & Apprentissage ALS',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['spark', 'machine-learning', 'streaming', 'production'],
) as dag:

    # 1. Vérification de la disponibilité de Kafka
    wait_for_kafka = BashOperator(
        task_id='wait_for_kafka',
        bash_command='nc -z kafka 29092',
        retries=5,
        retry_delay=timedelta(seconds=15)
    )

    # ---------------------------------------------------------
    # BRANCHE 1 : LE FLUX DE DONNÉES (SIMULATION TEMPS RÉEL)
    # ---------------------------------------------------------
    ingest_data = BashOperator(
        task_id='ingest_data_producer',
        bash_command='python /opt/airflow/src/producer/kafka_producer.py',
    )

    # ---------------------------------------------------------
    # BRANCHE 2 : INTELLIGENCE ARTIFICIELLE & INFERENCE
    # ---------------------------------------------------------
    # Soumission au cluster Spark (et non en local)
    train_model = SparkSubmitOperator(
        task_id='train_als_model',
        conn_id='spark_default', 
        application='/opt/airflow/src/spark/train_model.py',
        name='Batch_Training_ALS',
        # SUPPRESSION DU MASTER='local[*]' POUR UTILISER LA CONN_ID OU CONF
        conf={
            'spark.master': 'spark://spark-master:7077', # Force l'utilisation du cluster Docker
            'spark.driver.memory': '1g',
            'spark.executor.memory': '2g', # Aligné avec ton docker-compose
            'spark.executor.cores': '2'
        },
        verbose=True
    )

    start_streaming = SparkSubmitOperator(
        task_id='start_realtime_streaming',
        conn_id='spark_default',
        application='/opt/airflow/src/spark/streaming_recommender.py',
        # Packages nécessaires pour lire Kafka depuis Spark Structured Streaming
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
        name='Streaming_Inference',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.driver.memory': '1g',
            'spark.executor.memory': '2g',
        },
        verbose=True
    )

    # =========================================================
    # L'ORCHESTRATION PARALLÈLE (LE SECRET D'UN BON DAG)
    # =========================================================
    
    # Le producer démarre dès que Kafka est prêt (Branche 1)
    wait_for_kafka >> ingest_data
    
    # L'entraînement démarre en parallèle. Le streaming attend que le modèle soit prêt (Branche 2)
    wait_for_kafka >> train_model >> start_streaming