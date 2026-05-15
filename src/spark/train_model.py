import os
import shutil
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chemins exacts
DATA_PATH = "file:///opt/spark/data/Reviews.csv"
MODEL_DIR = "file:///opt/spark/models/als_model"
USER_INDEXER_PATH = "file:///opt/spark/models/user_indexer"
ITEM_INDEXER_PATH = "file:///opt/spark/models/item_indexer"

def main():
    logger.info("Initialisation de la SparkSession connectée au cluster...")
    spark = SparkSession.builder \
        .appName("ProductRecommendationTrainingOptimized") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.speculation", "false") \
        .getOrCreate()

    try:
        logger.info(f"Chargement des données depuis {DATA_PATH}...")
        print(f"DEBUG: Tentative de lecture du fichier au chemin exact -> {DATA_PATH}")
        df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
        
        df = df.withColumn("Time", col("Time").cast("long"))
        df = df.withColumn("Id", col("Id").cast("int"))

        # Split logique deterministe pour eviter le data leakage (batch 60%).
        df = df.filter(col("Id") % 10 < 6)
        logger.info("Split logique 60%% applique (Id %% 10 < 6)")

        df = df.select(
            df['UserId'].cast('string'),
            df['ProductId'].cast('string'),
            df['Score'].cast('float')
        )
        
        logger.info("Prétraitement : Suppression des valeurs nulles et doublons...")
        df = df.dropna(subset=['UserId', 'ProductId', 'Score'])
        df = df.dropDuplicates(['UserId', 'ProductId'])

        logger.info("Filtrage strict : utilisateurs (>= 5 avis) et produits (>= 5 notes)...")
        # Garder uniquement les utilisateurs avec >= 5 avis
        user_counts = df.groupBy("UserId").count().filter(F.col("count") >= 5).select("UserId")
        df = df.join(user_counts, "UserId", "inner")
        
        # Garder uniquement les produits avec >= 5 notes
        item_counts = df.groupBy("ProductId").count().filter(F.col("count") >= 5).select("ProductId")
        df = df.join(item_counts, "ProductId", "inner")

        logger.info("Prétraitement : Transformation avec StringIndexer...")
        
        # 1. OPTIMISATION : Mise en cache des données après nettoyage pour éviter de tout recalculer
        df.cache()
        logger.info(f"Nombre de lignes après nettoyage et filtrage : {df.count()}")

        user_indexer = StringIndexer(inputCol="UserId", outputCol="user_index", handleInvalid="keep")
        item_indexer = StringIndexer(inputCol="ProductId", outputCol="item_index", handleInvalid="keep")

        user_indexer_model = user_indexer.fit(df)
        df = user_indexer_model.transform(df)

        item_indexer_model = item_indexer.fit(df)
        df = item_indexer_model.transform(df)

        # 3. Séparation stricte : 90% (Train/Validation) et 10% (Test final)
        logger.info("Split des données : 90% (Train+Validation), 10% (Test)...")
        # On extrait les 10% de données non rencontrées pour le test final de l'architecture
        (train_val_data, test_data) = df.randomSplit([0.9, 0.1], seed=42)

        als = ALS(
            userCol="user_index",
            itemCol="item_index",
            ratingCol="Score",
            coldStartStrategy="drop"
        )
            
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="Score",
            predictionCol="prediction"
        )
        
        # 4. Ajustement des hyperparamètres pour respecter l'énoncé
        logger.info("Configuration de la grille d'hyperparamètres (légère)...")
        param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 20]) \
            .addGrid(als.regParam, [0.1, 0.05]) \
            .build()

        # TrainValidationSplit va diviser le set de 90% en interne (trainRatio=0.88 équivaut à ~80% train / ~10% validation)
        tvs = TrainValidationSplit(
            estimator=als,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            trainRatio=0.88 
        )

        logger.info("Entraînement et validation du modèle ALS en cours...")
        tvs_model = tvs.fit(train_val_data)
        
        # Extraction du meilleur modèle trouvé par la grille
        best_model = tvs_model.bestModel

        # 5. Évaluation sur les 10% restants (strictement inconnus)
        logger.info("Évaluation finale du modèle sur le set de test (10%)...")
        test_predictions = best_model.transform(test_data)
        final_rmse = evaluator.evaluate(test_predictions)
        logger.info(f"*** RMSE FINAL SUR LE SET DE TEST = {final_rmse} ***")

        
        logger.info("Sauvegarde des modèles en cours...")
        # Bloc de sécurité : création du dossier parent et nettoyage des anciens modèles pour éviter les plantages d'écriture
        base_models_dir = "/opt/spark/models"
        if not os.path.exists(base_models_dir):
            os.makedirs(base_models_dir, exist_ok=True)
        
        # Sauvegarde du modèle
        best_model.write().overwrite().save(MODEL_DIR)
        logger.info(f"Best Modèle ALS sauvegardé vers : {MODEL_DIR}")
        
        user_indexer_model.write().overwrite().save(USER_INDEXER_PATH)
        logger.info(f"User Indexer sauvegardé vers : {USER_INDEXER_PATH}")
        
        item_indexer_model.write().overwrite().save(ITEM_INDEXER_PATH)
        logger.info(f"Item Indexer sauvegardé vers : {ITEM_INDEXER_PATH}")

        logger.info("Pipeline d'entraînement optimisé terminé avec succès.")
        
    except Exception as e:
        logger.error(f"Erreur rencontrée lors de l'entraînement : {str(e)}")
        import sys
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("SparkSession arrêtée.")

if __name__ == "__main__":
    main()
