# Import des bibliothèques nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, desc, year, month, dayofmonth
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
import os
import matplotlib.pyplot as plt
import seaborn as sns

# Création de la session Spark
spark = SparkSession.builder \
    .appName("MovieLens Data Processing") \
    .config("spark.sql.shuffle.partitions", 10) \
    .getOrCreate()

# Chemins HDFS
hdfs_path = "hdfs://localhost:9000"
hdfs_ratings_path = f"{hdfs_path}/user/root/movielens/ratings.csv"
hdfs_movies_path = f"{hdfs_path}/user/root/movielens/movies.csv"
hdfs_output_path = f"{hdfs_path}/user/root/movielens/processed"

def import_csv_to_hdfs(local_path, hdfs_path):
    """
    Importe un fichier CSV local vers HDFS
    """
    # Vérifier si le dossier parent existe dans HDFS
    parent_dir = os.path.dirname(hdfs_path)
    os.system(f"hdfs dfs -mkdir -p {parent_dir}")
    
    # Copier le fichier
    os.system(f"hdfs dfs -put -f {local_path} {hdfs_path}")
    print(f"Fichier {local_path} importé vers {hdfs_path}")

def check_data_quality(df, name):
    """
    Vérification de la qualité des données
    """
    print(f"\n===== Vérification de la qualité des données : {name} =====")
    
    # Affichage du schéma
    print("\nSchéma:")
    df.printSchema()
    
    # Nombre de lignes et colonnes
    num_rows = df.count()
    num_cols = len(df.columns)
    print(f"\nNombre de lignes: {num_rows}")
    print(f"Nombre de colonnes: {num_cols}")
    
    # Vérification des doublons
    num_duplicates = num_rows - df.dropDuplicates().count()
    print(f"\nNombre de doublons: {num_duplicates}")
    
    # Valeurs manquantes par colonne
    print("\nValeurs manquantes par colonne:")
    df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns]).show()
    
    # Statistiques de base pour les colonnes numériques
    print("\nStatistiques de base:")
    df.describe().show()
    
    # Affichage de quelques exemples
    print("\nExemples de données:")
    df.show(5)
    
    return df

def preprocess_ratings(df):
    """
    Prétraitement des données de ratings
    """
    print("\n===== Prétraitement des données de ratings =====")
    
    # Conversion des types
    df = df.withColumn("userId", col("userId").cast(IntegerType())) \
           .withColumn("movieId", col("movieId").cast(IntegerType())) \
           .withColumn("rating", col("rating").cast(DoubleType())) \
           .withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    # Filtrage des notes extrêmes (si nécessaire)
    print("\nDistribution des ratings avant filtrage:")
    df.groupBy("rating").count().orderBy("rating").show()
    
    # Filtrer les notes inférieures à 0.5 ou supérieures à 5 (si elles existent)
    filtered_df = df.filter((col("rating") >= 0.5) & (col("rating") <= 5.0))
    
    print("\nDistribution des ratings après filtrage:")
    filtered_df.groupBy("rating").count().orderBy("rating").show()
    
    # Nombre de ratings par utilisateur
    print("\nTop 10 utilisateurs avec le plus de ratings:")
    filtered_df.groupBy("userId").count().orderBy(desc("count")).show(10)
    
    return filtered_df

def preprocess_movies(df):
    """
    Prétraitement des données de films
    """
    print("\n===== Prétraitement des données de films =====")
    
    # Conversion des types
    df = df.withColumn("movieId", col("movieId").cast(IntegerType()))
    
    # Vérification des genres
    print("\nDistribution des genres:")
    df.select("genres").groupBy("genres").count().orderBy(desc("count")).show(10)
    
    return df

def join_dataframes(ratings_df, movies_df):
    """
    Jointure des DataFrames ratings et movies
    """
    print("\n===== Jointure des DataFrames =====")
    
    joined_df = ratings_df.join(movies_df, "movieId", "inner")
    
    print("\nSchéma du DataFrame joint:")
    joined_df.printSchema()
    
    print("\nExemple de données jointes:")
    joined_df.show(5)
    
    # Films les mieux notés (avec au moins 100 évaluations)
    print("\nTop 10 des films les mieux notés (avec au moins 100 évaluations):")
    ratings_count = ratings_df.groupBy("movieId").count().withColumnRenamed("count", "num_ratings")
    avg_ratings = ratings_df.groupBy("movieId").agg({"rating": "avg"}).withColumnRenamed("avg(rating)", "avg_rating")
    
    top_movies = avg_ratings.join(ratings_count, "movieId") \
                            .filter(col("num_ratings") >= 100) \
                            .join(movies_df, "movieId") \
                            .select("movieId", "title", "genres", "avg_rating", "num_ratings") \
                            .orderBy(desc("avg_rating"), desc("num_ratings"))
    
    top_movies.show(10)
    
    return joined_df

def partition_and_save(df, output_path):
    """
    Partitionnement et sauvegarde des données
    """
    print("\n===== Partitionnement et sauvegarde des données =====")
    
    # Partitionnement par année, mois et jour de la date du rating
    partitioned_df = df.withColumn("year", year(col("timestamp"))) \
                       .withColumn("month", month(col("timestamp"))) \
                       .withColumn("day", dayofmonth(col("timestamp")))
    
    # Vérification de la répartition des partitions
    print("\nRépartition des données par année:")
    partitioned_df.groupBy("year").count().orderBy("year").show()
    
    # Sauvegarde en format parquet avec partitionnement
    partitioned_df.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"\nDonnées sauvegardées avec partitionnement par année/mois dans: {output_path}")

def main():
    # Création des répertoires HDFS si nécessaires
    os.system("hdfs dfs -mkdir -p /user/root/movielens")
    
    # Importer les fichiers dans HDFS
    # Note: Supposons que les fichiers sont disponibles localement dans le répertoire /data
    import_csv_to_hdfs("/data/ratings.csv", hdfs_ratings_path)
    import_csv_to_hdfs("/data/movies.csv", hdfs_movies_path)
    
    # Charger les données depuis HDFS
    ratings_df = spark.read.option("header", "true").csv(hdfs_ratings_path)
    movies_df = spark.read.option("header", "true").csv(hdfs_movies_path)
    
    # Vérification de la qualité des données
    ratings_df = check_data_quality(ratings_df, "ratings.csv")
    movies_df = check_data_quality(movies_df, "movies.csv")
    
    # Prétraitement
    ratings_df = preprocess_ratings(ratings_df)
    movies_df = preprocess_movies(movies_df)
    
    # Jointure
    joined_df = join_dataframes(ratings_df, movies_df)
    
    # Partitionnement et sauvegarde
    partition_and_save(joined_df, hdfs_output_path)
    
    print("\n===== Traitement terminé avec succès =====")

if __name__ == "__main__":
    main()
