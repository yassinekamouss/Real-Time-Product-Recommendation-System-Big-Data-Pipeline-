import os
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chemins
DATA_PATH = "/opt/spark/data/Reviews.csv"
MODEL_DIR = "/opt/spark/models/als_model"
USER_INDEXER_PATH = "/opt/spark/models/user_indexer"
ITEM_INDEXER_PATH = "/opt/spark/models/item_indexer"

def main():
    logger.info("Initialisation de la SparkSession...")
    spark = SparkSession.builder \
        .appName("ProductRecommendationTrainingOptimized") \
        .getOrCreate()

    try:
        logger.info(f"Chargement des données depuis {DATA_PATH}...")
        df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
        
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
        user_indexer = StringIndexer(inputCol="UserId", outputCol="user_index", handleInvalid="keep")
        item_indexer = StringIndexer(inputCol="ProductId", outputCol="item_index", handleInvalid="keep")

        user_indexer_model = user_indexer.fit(df)
        df = user_indexer_model.transform(df)

        item_indexer_model = item_indexer.fit(df)
        df = item_indexer_model.transform(df)

        logger.info("Split des données : 80% entraînement, 20% test...")
        (training_data, test_data) = df.randomSplit([0.8, 0.2], seed=42)

        logger.info("Configuration de ALS et de la validation croisée...")
        als = ALS(
            userCol="user_index",
            itemCol="item_index",
            ratingCol="Score",
            coldStartStrategy="drop"
        )
        
        # 1. ParamGridBuilder
        param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 50, 100]) \
            .addGrid(als.regParam, [0.01, 0.1, 1.0]) \
            .build()
            
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="Score",
            predictionCol="prediction"
        )
        
        # 2. CrossValidator (3 folds)
        cv = CrossValidator(
            estimator=als,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )

        logger.info("Entraînement et optimisation des hyperparamètres (Grid Search + 3 Folds)...")
        cv_model = cv.fit(training_data)
        
        # Extraction du meilleur modèle
        best_model = cv_model.bestModel

        best_rank = best_model._java_obj.parent().getRank()
        best_reg = best_model._java_obj.parent().getRegParam()
        logger.info(f"Meilleurs hyperparamètres identifiés : rank = {best_rank}, regParam = {best_reg}")

        logger.info("Évaluation du best model sur les données de test final...")
        predictions = best_model.transform(test_data)
        rmse = evaluator.evaluate(predictions)
        logger.info(f"Root-mean-square error (RMSE) optimisé sur le set de test = {rmse}")

        logger.info("Sauvegarde des modèles en cours...")
        # Sauvegarde du meilleur modèle
        best_model.write().overwrite().save(MODEL_DIR)
        logger.info(f"Best Modèle ALS sauvegardé vers : {MODEL_DIR}")
        
        user_indexer_model.write().overwrite().save(USER_INDEXER_PATH)
        logger.info(f"User Indexer sauvegardé vers : {USER_INDEXER_PATH}")
        
        item_indexer_model.write().overwrite().save(ITEM_INDEXER_PATH)
        logger.info(f"Item Indexer sauvegardé vers : {ITEM_INDEXER_PATH}")

        logger.info("Pipeline d'entraînement optimisé terminé avec succès.")
        
    except Exception as e:
        logger.error(f"Erreur rencontrée lors de l'entraînement : {str(e)}")
    finally:
        spark.stop()
        logger.info("SparkSession arrêtée.")

if __name__ == "__main__":
    main()
