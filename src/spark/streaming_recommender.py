import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
import pyspark.sql.functions as F
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.feature import StringIndexerModel

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("StreamingRecommender")

# Constantes de configuration
KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "user-ratings"
MODEL_DIR = "/opt/spark/models/als_model"
USER_INDEXER_PATH = "/opt/spark/models/user_indexer"
ITEM_INDEXER_PATH = "/opt/spark/models/item_indexer"  # Au besoin
CHECKPOINT_DIR = "/opt/spark/models/checkpoints/"

DB_URL = "jdbc:postgresql://postgres:5432/airflow"
DB_PROPERTIES = {
    "user": "airflow",
    "password": "airflow_pass",
    "driver": "org.postgresql.Driver"
}

def main():
    logger.info("Initialisation de la SparkSession streaming...")
    spark = SparkSession.builder \
        .appName("RealTimeProductRecommender") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("Chargement des modèles ML (ALS et StringIndexer)...")
        als_model = ALSModel.load(MODEL_DIR)
        user_indexer_model = StringIndexerModel.load(USER_INDEXER_PATH)
        
        # Optionnel: on peut charger l'item_indexer_model pour récupérer les vrais ProductId, 
        # mais ici on se contente de la recommandation sous sa forme indexée pour la rapidité.
        # item_indexer_model = StringIndexerModel.load(ITEM_INDEXER_PATH)

        # Définition du schéma JSON attendu
        schema = StructType([
            StructField("UserId", StringType(), True),
            StructField("ProductId", StringType(), True),
            StructField("Score", FloatType(), True),
            StructField("Time", LongType(), True)
        ])

        logger.info(f"Connexion au stream Kafka ({KAFKA_BROKER}, topic: {KAFKA_TOPIC})...")
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

        # Parsing des messages JSON
        parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
            .select(F.from_json(F.col("value"), schema).alias("data")) \
            .select("data.*")

        def process_batch(batch_df, epoch_id):
            """
            Traitement de chaque micro-batch:
            1. Extraction des utilisateurs uniques
            2. Transformation de UserId -> user_index
            3. Génération des top-5 recommandations
            4. Sauvegarde dans PostgreSQL.
            """
            if batch_df.isEmpty():
                return
            
            logger.info(f"Traitement du micro-batch {epoch_id}...")

            # 1. Extraction des utilisateurs uniques du batch
            unique_users_df = batch_df.select("UserId").distinct()

            # 2. Transformation en user_index (numeric)
            indexed_users_df = user_indexer_model.transform(unique_users_df)

            # Pour se prémunir d'erreurs dues aux nouveaux utilisateurs non vus à l'entraînement,
            # on filtre ceux dont l'index a bien été généré
            valid_users_df = indexed_users_df.dropna(subset=["user_index"])

            # 3. Génération des 5 recommandations par utilisateur
            if not valid_users_df.isEmpty():
                recs = als_model.recommendForUserSubset(valid_users_df, 5)

                # Formatage de l'output pour PostgreSQL
                # recs contient [user_index, recommendations (array of structs)]
                # On joint avec valid_users_df pour récupérer le UserId original (string)
                formatted_recs = recs.join(valid_users_df, "user_index", "inner") \
                    .select(
                        F.col("UserId"),
                        F.to_json(F.col("recommendations")).alias("recommendations") # Stockage SQL sous format text/json
                    )

                # 4. Écriture en mode append dans la base PostgreSQL logicielle
                formatted_recs.write \
                    .jdbc(url=DB_URL, table="user_recommendations", mode="append", properties=DB_PROPERTIES)
                
                logger.info(f"Recommandations pour le batch {epoch_id} insérées dans PostgreSQL avec succès.")

        logger.info("Démarrage du query de streaming...")
        query = parsed_stream.writeStream \
            .outputMode("update") \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", CHECKPOINT_DIR) \
            .start()

        query.awaitTermination()

    except Exception as e:
        logger.error(f"Une erreur critique est survenue dans le job de streaming: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
