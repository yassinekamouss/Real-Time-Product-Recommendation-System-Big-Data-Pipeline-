import os
import json
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.feature import StringIndexerModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "airflow")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow_pass")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-ratings")

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

MODEL_DIR = "file:///opt/spark/models/als_model"
USER_INDEXER_PATH = "file:///opt/spark/models/user_indexer"
ITEM_INDEXER_PATH = "file:///opt/spark/models/item_indexer"
CHECKPOINT_DIR = "file:///opt/spark/models/checkpoints/streaming"

TOP_N = int(os.getenv("RECOMMEND_TOP_N", "5"))

def init_db():
    """
    CORRECTION 2 : Initialise la table dès le lancement.
    Ainsi, l'API ne plantera jamais, même si Kafka est vide !
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_recommendations (
                "UserId" VARCHAR(255) PRIMARY KEY,
                recommendations TEXT
            )
        """)
        conn.commit()
        cur.close()
        logger.info("Table PostgreSQL 'user_recommendations' vérifiée/créée avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors de la création de la table : {e}")
    finally:
        if conn:
            conn.close()

def save_to_postgres(batch_df, batch_id, item_labels):
    if batch_df.rdd.isEmpty():
        return

    records = batch_df.collect()
    conn = None

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()

        for row in records:
            user_id = str(row["UserId"])
            recs = []
            for rec in row["recommendations"]:
                item_index = int(rec.item_index)
                if 0 <= item_index < len(item_labels):
                    recs.append({
                        "ProductId": item_labels[item_index],
                        "score": float(rec.rating)
                    })

            recs_json = json.dumps(recs)
            cur.execute("""
                INSERT INTO user_recommendations ("UserId", recommendations)
                VALUES (%s, %s)
                ON CONFLICT ("UserId") DO UPDATE
                SET recommendations = EXCLUDED.recommendations;
            """, (user_id, recs_json))

        conn.commit()
        logger.info(f"Batch {batch_id} : {len(records)} recommandations mises à jour en base.")
        cur.close()
    except Exception as e:
        logger.error(f"Erreur d'insertion PostgreSQL : {e}")
    finally:
        if conn:
            conn.close()

def main():
    # On crée la table avant même de lancer Spark
    init_db()

    spark = SparkSession.builder \
        .appName("RealTimeRecommender") \
        .master(SPARK_MASTER) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("Chargement du modele ALS et des StringIndexers...")
        als_model = ALSModel.load(MODEL_DIR)
        user_indexer = StringIndexerModel.load(USER_INDEXER_PATH)
        item_indexer = StringIndexerModel.load(ITEM_INDEXER_PATH)
        item_labels = item_indexer.labels
        logger.info("Modèles chargés avec succès !")
    except Exception as e:
        logger.error(f"Modeles introuvables. Entrainement batch a verifier. Erreur: {e}")
        import sys; sys.exit(1)

    max_user_index = None
    try:
        max_user_index = als_model.userFactors.agg(F.max("id")).collect()[0][0]
    except Exception:
        max_user_index = None

    schema = StructType([
        StructField("UserId", StringType(), True),
        StructField("ProductId", StringType(), True),
        StructField("Score", FloatType(), True),
        StructField("Time", LongType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 5000) \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS value") \
        .select(F.from_json(F.col("value"), schema).alias("data")) \
        .select("data.*") \
        .filter(F.col("UserId").isNotNull())

    def process_micro_batch(batch_df, batch_id):
            # --- LE RADAR EST ICI ---
            total_messages = batch_df.count()
            logger.info(f"========== MICRO BATCH {batch_id} REÇU | Messages: {total_messages} ==========")

            if batch_df.rdd.isEmpty():
                return

            unique_users = batch_df.select("UserId").distinct()
            logger.info(f"-> Utilisateurs uniques trouvés dans Kafka : {unique_users.count()}")

            # Transformation via le modèle
            try:
                indexed_users = user_indexer.transform(unique_users).select("UserId", "user_index")
                indexed_users = indexed_users.filter(F.col("user_index").isNotNull())
                logger.info(f"-> Utilisateurs reconnus par le modèle ML : {indexed_users.count()}")

                if max_user_index is not None:
                    indexed_users = indexed_users.filter(F.col("user_index") <= F.lit(max_user_index))

                if indexed_users.rdd.isEmpty():
                    logger.info("-> Aucun utilisateur n'a pu être traité. Annulation de l'insertion.")
                    return

                recommendations = als_model.recommendForUserSubset(indexed_users, TOP_N)
                final_recs = recommendations.join(indexed_users, "user_index") \
                    .select("UserId", "recommendations")

                logger.info(f"-> Envoi vers PostgreSQL de {final_recs.count()} lignes...")
                save_to_postgres(final_recs, batch_id, item_labels)
                
            except Exception as e:
                logger.error(f"ERREUR FATALE PENDANT LE MICRO-BATCH : {e}")

    query = parsed_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="120 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()