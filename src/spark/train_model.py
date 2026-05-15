import os
from datetime import datetime
import logging
import shutil
import json
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
        df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
        
        df = df.withColumn("Time", col("Time").cast("long"))
        df = df.withColumn("Id", col("Id").cast("int"))

        # Split logique deterministe pour eviter le data leakage
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
        user_counts = df.groupBy("UserId").count().filter(F.col("count") >= 5).select("UserId")
        df = df.join(user_counts, "UserId", "inner")
        
        item_counts = df.groupBy("ProductId").count().filter(F.col("count") >= 5).select("ProductId")
        df = df.join(item_counts, "ProductId", "inner")

        logger.info("Prétraitement : Transformation avec StringIndexer...")
        df.cache()
        
        user_indexer = StringIndexer(inputCol="UserId", outputCol="user_index", handleInvalid="keep")
        item_indexer = StringIndexer(inputCol="ProductId", outputCol="item_index", handleInvalid="keep")

        user_indexer_model = user_indexer.fit(df)
        df = user_indexer_model.transform(df)

        item_indexer_model = item_indexer.fit(df)
        df = item_indexer_model.transform(df)

        logger.info("Split des données : 90% (Train+Validation), 10% (Test)...")
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
        
        logger.info("Configuration de la grille d'hyperparamètres...")
        param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 20]) \
            .addGrid(als.regParam, [0.1, 0.05]) \
            .build()

        tvs = TrainValidationSplit(
            estimator=als,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            trainRatio=0.88 
        )

        logger.info("Entraînement et validation du modèle ALS en cours...")
        tvs_model = tvs.fit(train_val_data)
        best_model = tvs_model.bestModel

        logger.info("Évaluation finale du modèle sur le set de test (10%)...")
        test_predictions = best_model.transform(test_data)
        final_rmse = evaluator.evaluate(test_predictions)
        logger.info(f"*** RMSE FINAL SUR LE SET DE TEST = {final_rmse} ***")

        # --- Extraction des métriques ---
        try:
            reg_param = best_model.getOrDefault(best_model.getParam("regParam"))
        except:
            reg_param = 0.1 

        metrics_data = {
            "rmse": final_rmse,
            "rank": best_model.rank,
            "regParam": reg_param,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info("Implémentation du Versioning et sauvegarde...")
        base_models_dir = "/opt/spark/models"
        archive_dir = os.path.join(base_models_dir, "archive")
        
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir, exist_ok=True)

        version_tag = datetime.now().strftime("v_%Y%m%d_%H%M%S")
        versioned_model_path = f"file://{archive_dir}/als_model_{version_tag}"
        best_model.write().save(versioned_model_path)
        logger.info(f"Version archivée sous : {versioned_model_path}")
        
        for path in [MODEL_DIR, USER_INDEXER_PATH, ITEM_INDEXER_PATH]:
            local_path = path.replace("file://", "")
            if os.path.exists(local_path):
                shutil.rmtree(local_path, ignore_errors=True)

        # 1. Sauvegarder avec Spark (crée les dossiers en root)
        best_model.write().overwrite().save(MODEL_DIR)
        user_indexer_model.write().overwrite().save(USER_INDEXER_PATH)
        item_indexer_model.write().overwrite().save(ITEM_INDEXER_PATH)

        # 2. LA CORRECTION CRITIQUE : Déverrouiller le dossier AVANT d'écrire le JSON
        os.system(f"chmod -R 777 {base_models_dir}")
        logger.info("Permissions 777 appliquées. Le dossier est modifiable par tous.")

        # 3. Écrire le fichier metrics.json avec Airflow
        metrics_file = os.path.join(MODEL_DIR.replace("file://", ""), "metrics.json")
        with open(metrics_file, "w") as f:
            json.dump(metrics_data, f, indent=4)
            
        logger.info(f"Modèles de production et métriques mis à jour avec succès.")
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