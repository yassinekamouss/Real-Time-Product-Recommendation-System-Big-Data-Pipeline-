import os
import glob
import shutil
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer

# Configuration du logging pour un suivi professionnel
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Initialisation de la SparkSession optimisée
    spark = SparkSession.builder \
        .appName("AmazonFineFoodPreprocess") \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()

    try:
        logger.info("Chargement des données source...")
        input_path = "/opt/spark/data/Reviews.csv"
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        # --- ÉTAPE 1 : Nettoyage des doublons ---
        # On garde l'avis le plus récent par utilisateur/produit pour éviter le bruit
        logger.info("Suppression des doublons (Time-based)...")
        window_spec = Window.partitionBy("UserId", "ProductId").orderBy(desc("Time"))
        df_unique = df.withColumn("rank", row_number().over(window_spec)) \
                      .filter(col("rank") == 1) \
                      .drop("rank")

        # --- ÉTAPE 2 : Filtrage des utilisateurs (Min 3 interactions) ---
        logger.info("Filtrage des utilisateurs actifs (interactions >= 3)...")
        user_counts = df_unique.groupBy("UserId").count()
        active_users = user_counts.filter(col("count") >= 3).select("UserId")
        
        # Filtrage par jointure interne
        df_filtered = df_unique.join(active_users, on="UserId", how="inner")

        # --- ÉTAPE 3 : Indexation pour ALS ---
        # On utilise les noms de colonnes cibles : user_index et item_index
        logger.info("Génération des index numériques (StringIndexer)...")
        indexer_user = StringIndexer(inputCol="UserId", outputCol="user_index")
        indexer_item = StringIndexer(inputCol="ProductId", outputCol="item_index")

        df_indexed = indexer_user.fit(df_filtered).transform(df_filtered)
        df_indexed = indexer_item.fit(df_indexed).transform(df_indexed)

        # --- ÉTAPE 4 : Sélection Finale & Type Casting ---
        # On ne garde que les colonnes strictement nécessaires
        logger.info("Sélection des colonnes finales et conversion du Score...")
        final_columns = ["Id", "ProductId", "UserId", "Score", "user_index", "item_index"]
        
        df_final = df_indexed.select(*final_columns) \
                             .withColumn("Score", col("Score").cast("double"))

        # --- ÉTAPE 5 : Exportation Propre ---
        output_base_dir = "/opt/spark/data_processing/output"
        temp_spark_dir = os.path.join(output_base_dir, "temp_spark_output")
        final_csv_path = os.path.join(output_base_dir, "processed_data.csv")

        logger.info(f"Exportation vers {final_csv_path}...")
        
        # Coalesce(1) pour garantir un fichier unique, puis nettoyage manuel du dossier Spark
        df_final.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(temp_spark_dir)

        # Récupération du fichier CSV et renommage
        part_file = glob.glob(f"{temp_spark_dir}/*.csv")[0]
        shutil.move(part_file, final_csv_path)
        shutil.rmtree(temp_spark_dir)
        
        logger.info("Processus terminé avec succès.")

    except Exception as e:
        logger.error(f"Erreur durant le processing : {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()