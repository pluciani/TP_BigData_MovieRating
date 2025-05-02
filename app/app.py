import streamlit as st
import numpy as np
import pymongo
from pymongo import MongoClient
import pickle
import os
from typing import List, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split
from pyspark.ml.recommendation import ALSModel

# Configuration de la page Streamlit
st.set_page_config(
    page_title="Recommandation de Films",
    page_icon="🎬",
    layout="wide"
)

# Titre de l'application
st.title("🎬 Système de Recommandation de Films")
st.markdown("Cette application prédit les notes que vous donneriez à des films basé sur votre profil.")

# Initialisation de la session Spark
@st.cache_resource
def get_spark_session():
    """Initialise et retourne une session Spark."""
    spark = SparkSession.builder \
        .appName("RecommendationApp") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    return spark

# Fonctions pour la connexion à MongoDB
@st.cache_resource
def get_mongo_client():
    """Établit la connexion avec MongoDB."""
    # Utilisez les variables d'environnement ou remplacez par vos informations de connexion
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    return MongoClient(mongo_uri)

@st.cache_data
def get_movies_from_db(limit: int = 1000) -> List[Dict[str, Any]]:
    """Récupère la liste des films depuis MongoDB et les charge dans un DataFrame Spark."""
    spark = get_spark_session()
    client = get_mongo_client()
    db = client["movies_db"]  # Remplacez par le nom de votre base de données
    collection = db["movies"]  # Remplacez par le nom de votre collection
    
    # Récupérer les films (avec une limite pour éviter de charger trop de données)
    movies = list(collection.find({}, {"_id": 1, "title": 1, "genres": 1, "year": 1}).limit(limit))
    
    # Convertir les ObjectId MongoDB en string pour faciliter le traitement
    for movie in movies:
        movie["_id"] = str(movie["_id"])
    
    # Créer un DataFrame Spark à partir des données MongoDB
    if movies:
        movies_df = spark.createDataFrame(movies)
        return movies_df
    else:
        # Créer un DataFrame vide avec la structure attendue
        return spark.createDataFrame([], schema="_id string, title string, genres string, year integer")

@st.cache_data
def get_user_ratings(user_id: str):
    """Récupère les notes données par un utilisateur spécifique."""
    spark = get_spark_session()
    client = get_mongo_client()
    db = client["movies_db"]
    collection = db["ratings"]
    
    # Récupérer les notes de l'utilisateur
    ratings = list(collection.find({"user_id": user_id}))
    
    # Convertir les ObjectId MongoDB en string
    for rating in ratings:
        if "_id" in rating:
            rating["_id"] = str(rating["_id"])
        rating["movie_id"] = str(rating["movie_id"])
    
    # Créer un DataFrame Spark à partir des données
    if ratings:
        ratings_df = spark.createDataFrame(ratings)
        return ratings_df
    else:
        # Créer un DataFrame vide avec la structure attendue
        return spark.createDataFrame([], schema="user_id string, movie_id string, rating double")

# Chargement du modèle ALS pré-entraîné
@st.cache_resource
def load_als_model():
    """Charge le modèle ALS pré-entraîné."""
    spark = get_spark_session()
    try:
        # Si vous utilisez le format natif de Spark ML pour sauvegarder le modèle
        model_path = "model/als_model"
        model = ALSModel.load(model_path)
        return model
    except Exception as e:
        st.error(f"Erreur lors du chargement du modèle ALS: {e}")
        try:
            # Alternative: si le modèle est stocké au format pickle
            with open('model/als_model.pkl', 'rb') as f:
                model = pickle.load(f)
            return model
        except FileNotFoundError:
            st.error("Modèle ALS non trouvé. Veuillez vérifier le chemin du fichier.")
            return None

# Fonction pour prédire les notes avec Spark
def predict_ratings_spark(user_id: str, movies_df, model):
    """Prédit les notes qu'un utilisateur donnerait à une liste de films avec Spark."""
    spark = get_spark_session()
    
    try:
        # Créer un DataFrame avec l'ID utilisateur et chaque ID de film
        user_data = []
        for movie_id in movies_df.select("_id").rdd.flatMap(lambda x: x).collect():
            user_data.append((user_id, movie_id))
        
        if not user_data:
            return spark.createDataFrame([], schema="user_id string, movie_id string, prediction double")
        
        # Créer un DataFrame pour la prédiction
        user_movie_df = spark.createDataFrame(user_data, ["user_id", "movie_id"])
        
        # Faire la prédiction avec le modèle ALS
        predictions = model.transform(user_movie_df)
        
        return predictions
    except Exception as e:
        st.error(f"Erreur lors de la prédiction avec Spark: {e}")
        
        # Fallback: création manuelle de prédictions par défaut
        default_predictions = [(user_id, row._id, 3.0) for row in movies_df.collect()]
        return spark.createDataFrame(default_predictions, ["user_id", "movie_id", "prediction"])

# Interface principale de l'application
def main():
    spark = get_spark_session()
    
    # Sidebar pour les paramètres utilisateur
    st.sidebar.header("Paramètres utilisateur")
    
    # Identification de l'utilisateur
    user_id = st.sidebar.text_input("ID Utilisateur", "nouveau_utilisateur")
    
    # Récupération des films
    all_movies_df = get_movies_from_db()
    
    # Extraction des genres uniques
    genres_df = all_movies_df.filter(col("genres").isNotNull())
    if genres_df.count() > 0:
        # Extraire et diviser les genres puis créer une liste de genres uniques
        genres_list = genres_df.select(explode(split(col("genres"), "\\|")).alias("genre")) \
                              .distinct() \
                              .collect()
        all_genres = sorted([row.genre for row in genres_list])
    else:
        all_genres = []
    
    # Filtre par genre
    selected_genres = st.sidebar.multiselect(
        "Filtrer par genre",
        all_genres,
        []
    )
    
    # Filtre par année
    years_df = all_movies_df.filter(col("year").isNotNull())
    if years_df.count() > 0:
        min_year = years_df.select(col("year")).agg({"year": "min"}).collect()[0][0]
        max_year = years_df.select(col("year")).agg({"year": "max"}).collect()[0][0]
    else:
        min_year, max_year = 1900, 2023
    
    year_range = st.sidebar.slider(
        "Plage d'années",
        min_value=int(min_year),
        max_value=int(max_year),
        value=(int(min_year), int(max_year))
    )
    
    # Nombre de films à afficher
    num_movies = st.sidebar.slider("Nombre de films à afficher", 5, 50, 10)
    
    # Filtrer les films selon les critères
    filtered_movies_df = all_movies_df
    
    # Filtre par genre
    if selected_genres:
        genre_condition = None
        for genre in selected_genres:
            if genre_condition is None:
                genre_condition = col("genres").contains(genre)
            else:
                genre_condition = genre_condition | col("genres").contains(genre)
        
        filtered_movies_df = filtered_movies_df.filter(genre_condition)
    
    # Filtre par année
    filtered_movies_df = filtered_movies_df.filter(
        (col("year") >= year_range[0]) & (col("year") <= year_range[1])
    )
    
    # Limiter le nombre de films
    filtered_movies_df = filtered_movies_df.limit(num_movies)
    
    # Charger le modèle ALS
    model = load_als_model()
    
    if model and filtered_movies_df.count() > 0:
        # Prédire les notes
        predictions_df = predict_ratings_spark(user_id, filtered_movies_df, model)
        
        # Récupérer les notes réelles si l'utilisateur existe
        if user_id != "nouveau_utilisateur":
            actual_ratings_df = get_user_ratings(user_id)
        else:
            actual_ratings_df = spark.createDataFrame([], schema="user_id string, movie_id string, rating double")
        
        # Joindre les prédictions avec les informations des films
        result_df = filtered_movies_df.join(
            predictions_df.select("movie_id", "prediction"),
            filtered_movies_df["_id"] == predictions_df["movie_id"],
            "left"
        )
        
        # Ajouter les notes réelles si disponibles
        if actual_ratings_df.count() > 0:
            result_df = result_df.join(
                actual_ratings_df.select("movie_id", "rating"),
                result_df["_id"] == actual_ratings_df["movie_id"],
                "left"
            )
        else:
            # Ajouter une colonne vide pour les notes réelles
            result_df = result_df.withColumn("rating", col("rating").cast("double"))
        
        # Trier par prédiction décroissante
        result_df = result_df.orderBy(col("prediction").desc())
        
        # Convertir en liste pour l'affichage dans Streamlit
        movies_list = result_df.collect()
        
        # Afficher les résultats
        st.header("Films recommandés pour vous")
        
        # Créer un tableau pour l'affichage
        display_data = []
        for movie in movies_list:
            display_data.append({
                "ID": movie._id,
                "Titre": movie.title if hasattr(movie, "title") else "Titre inconnu",
                "Genres": movie.genres if hasattr(movie, "genres") else "",
                "Année": movie.year if hasattr(movie, "year") else "",
                "Note prédite": round(float(movie.prediction) if hasattr(movie, "prediction") else 3.0, 1),
                "Note réelle": float(movie.rating) if hasattr(movie, "rating") and movie.rating is not None else None
            })
        
        # Afficher le tableau
        st.dataframe(display_data, use_container_width=True)
        
        # Section pour noter de nouveaux films
        st.header("Noter un film")
        
        if display_data:
            movie_titles = [movie["Titre"] for movie in display_data]
            selected_movie_index = st.selectbox(
                "Sélectionnez un film à noter",
                range(len(movie_titles)),
                format_func=lambda i: movie_titles[i]
            )
            
            selected_movie = display_data[selected_movie_index]
            selected_movie_id = selected_movie["ID"]
            
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Titre**: {selected_movie['Titre']}")
                st.write(f"**Genres**: {selected_movie['Genres']}")
                st.write(f"**Année**: {selected_movie['Année']}")
            
            with col2:
                st.write(f"**Note prédite**: {selected_movie['Note prédite']}/5")
                new_rating = st.slider("Votre note", 0.5, 5.0, 3.0, 0.5)
                
                if st.button("Soumettre la note"):
                    try:
                        # Enregistrer la note dans MongoDB
                        client = get_mongo_client()
                        db = client["movies_db"]
                        ratings_collection = db["ratings"]
                        
                        # Mettre à jour ou insérer la note
                        ratings_collection.update_one(
                            {"user_id": user_id, "movie_id": selected_movie_id},
                            {"$set": {"rating": new_rating}},
                            upsert=True
                        )
                        
                        st.success(f"Note de {new_rating}/5 enregistrée pour {selected_movie['Titre']} !")
                        
                        # Invalider le cache pour forcer un rechargement des données
                        st.cache_data.clear()
                        st.rerun()  # Actualiser l'application
                    except Exception as e:
                        st.error(f"Erreur lors de l'enregistrement de la note: {e}")
        else:
            st.warning("Aucun film disponible à noter.")
    else:
        if not model:
            st.error("Impossible de charger le modèle de recommandation. Veuillez vérifier la configuration.")
        else:
            st.warning("Aucun film ne correspond aux critères sélectionnés.")

if __name__ == "__main__":
    main()
