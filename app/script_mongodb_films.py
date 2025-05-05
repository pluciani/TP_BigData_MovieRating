import pymongo
import random
from datetime import datetime
import pandas as pd
import json
from bson import ObjectId

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Supprimer la base de données si elle existe déjà
client.drop_database("movies_db")

# Créer/accéder à la base de données
db = client["movies_db"]

# Données de test pour les films
movies_data = [
    {"_id": ObjectId(), "title": "The Shawshank Redemption", "year": 1994, "genres": ["Drame"], "director": "Frank Darabont", "popularity": 9.2},
    {"_id": ObjectId(), "title": "Le Parrain", "year": 1972, "genres": ["Crime", "Drame"], "director": "Francis Ford Coppola", "popularity": 9.1},
    {"_id": ObjectId(), "title": "Pulp Fiction", "year": 1994, "genres": ["Crime", "Drame"], "director": "Quentin Tarantino", "popularity": 8.9},
    {"_id": ObjectId(), "title": "Le Seigneur des Anneaux: Le Retour du Roi", "year": 2003, "genres": ["Aventure", "Fantastique"], "director": "Peter Jackson", "popularity": 8.9},
    {"_id": ObjectId(), "title": "Fight Club", "year": 1999, "genres": ["Drame"], "director": "David Fincher", "popularity": 8.8},
    {"_id": ObjectId(), "title": "Forrest Gump", "year": 1994, "genres": ["Drame", "Romance"], "director": "Robert Zemeckis", "popularity": 8.8},
    {"_id": ObjectId(), "title": "Inception", "year": 2010, "genres": ["Action", "Science-Fiction"], "director": "Christopher Nolan", "popularity": 8.7},
    {"_id": ObjectId(), "title": "The Matrix", "year": 1999, "genres": ["Action", "Science-Fiction"], "director": "Lana et Lilly Wachowski", "popularity": 8.7},
    {"_id": ObjectId(), "title": "Interstellar", "year": 2014, "genres": ["Aventure", "Science-Fiction"], "director": "Christopher Nolan", "popularity": 8.6},
    {"_id": ObjectId(), "title": "The Dark Knight", "year": 2008, "genres": ["Action", "Crime"], "director": "Christopher Nolan", "popularity": 9.0},
    {"_id": ObjectId(), "title": "Django Unchained", "year": 2012, "genres": ["Western", "Drame"], "director": "Quentin Tarantino", "popularity": 8.4},
    {"_id": ObjectId(), "title": "Le Voyage de Chihiro", "year": 2001, "genres": ["Animation", "Aventure"], "director": "Hayao Miyazaki", "popularity": 8.6},
    {"_id": ObjectId(), "title": "Le Silence des Agneaux", "year": 1991, "genres": ["Crime", "Thriller"], "director": "Jonathan Demme", "popularity": 8.6},
    {"_id": ObjectId(), "title": "Titanic", "year": 1997, "genres": ["Drame", "Romance"], "director": "James Cameron", "popularity": 7.9},
    {"_id": ObjectId(), "title": "Gladiator", "year": 2000, "genres": ["Action", "Drame"], "director": "Ridley Scott", "popularity": 8.5},
    {"_id": ObjectId(), "title": "La Vie est Belle", "year": 1997, "genres": ["Comédie", "Drame"], "director": "Roberto Benigni", "popularity": 8.6},
    {"_id": ObjectId(), "title": "Les Infiltrés", "year": 2006, "genres": ["Crime", "Drame"], "director": "Martin Scorsese", "popularity": 8.5},
    {"_id": ObjectId(), "title": "Parasite", "year": 2019, "genres": ["Thriller", "Drame"], "director": "Bong Joon-ho", "popularity": 8.5},
    {"_id": ObjectId(), "title": "Joker", "year": 2019, "genres": ["Crime", "Drame"], "director": "Todd Phillips", "popularity": 8.4},
    {"_id": ObjectId(), "title": "Avengers: Endgame", "year": 2019, "genres": ["Action", "Aventure"], "director": "Anthony et Joe Russo", "popularity": 8.4},
    {"_id": ObjectId(), "title": "La La Land", "year": 2016, "genres": ["Comédie", "Drame", "Musical"], "director": "Damien Chazelle", "popularity": 8.0},
    {"_id": ObjectId(), "title": "Dune", "year": 2021, "genres": ["Science-Fiction", "Aventure"], "director": "Denis Villeneuve", "popularity": 8.1},
    {"_id": ObjectId(), "title": "The Batman", "year": 2022, "genres": ["Action", "Crime"], "director": "Matt Reeves", "popularity": 7.9},
    {"_id": ObjectId(), "title": "Spider-Man: No Way Home", "year": 2021, "genres": ["Action", "Aventure"], "director": "Jon Watts", "popularity": 8.2},
    {"_id": ObjectId(), "title": "Oppenheimer", "year": 2023, "genres": ["Biographie", "Drame"], "director": "Christopher Nolan", "popularity": 8.5},
    {"_id": ObjectId(), "title": "Barbie", "year": 2023, "genres": ["Comédie", "Aventure"], "director": "Greta Gerwig", "popularity": 7.0},
    {"_id": ObjectId(), "title": "Tout Simplement Noir", "year": 2020, "genres": ["Comédie"], "director": "Jean-Pascal Zadi", "popularity": 6.5},
    {"_id": ObjectId(), "title": "Intouchables", "year": 2011, "genres": ["Comédie", "Drame"], "director": "Olivier Nakache et Éric Toledano", "popularity": 8.5},
    {"_id": ObjectId(), "title": "Avatar", "year": 2009, "genres": ["Action", "Aventure", "Science-Fiction"], "director": "James Cameron", "popularity": 7.9},
    {"_id": ObjectId(), "title": "The Social Network", "year": 2010, "genres": ["Biographie", "Drame"], "director": "David Fincher", "popularity": 7.7}
]

# Créer la collection movies et insérer les données
movies_collection = db["movies"]
movies_collection.insert_many(movies_data)
print(f"Collection 'movies' créée avec {movies_collection.count_documents({})} films")

# Créer des utilisateurs fictifs
users_data = []
for i in range(1, 11):
    users_data.append({
        "_id": f"user{i}",
        "name": f"Utilisateur {i}",
        "email": f"user{i}@example.com",
        "registration_date": datetime.now(),
        "preferences": {
            "genres": random.sample(["Action", "Aventure", "Comédie", "Crime", "Drame", "Fantastique", "Science-Fiction", "Thriller", "Romance", "Animation"], k=random.randint(2, 5))
        }
    })

# Créer la collection users et insérer les données
users_collection = db["users"]
users_collection.insert_many(users_data)
print(f"Collection 'users' créée avec {users_collection.count_documents({})} utilisateurs")

# Générer des évaluations aléatoires pour les utilisateurs
ratings_data = []
movies_list = list(movies_collection.find({}, {"_id": 1, "title": 1}))

for user in users_data:
    # Chaque utilisateur évalue entre 5 et 15 films
    num_ratings = random.randint(5, 15)
    # Sélectionner des films au hasard pour évaluation
    user_movies = random.sample(movies_list, num_ratings)
    
    for movie in user_movies:
        # Générer une note entre 1 et 5 avec un pas de 0.5
        rating = round(random.uniform(1, 5) * 2) / 2
        
        ratings_data.append({
            "user_id": user["_id"],
            "movie_id": movie["_id"],
            "movie_title": movie["title"],
            "rating": rating,
            "timestamp": datetime.now()
        })

# Créer la collection ratings et insérer les données
ratings_collection = db["ratings"]
ratings_collection.insert_many(ratings_data)
print(f"Collection 'ratings' créée avec {ratings_collection.count_documents({})} évaluations")

# Générer des recommandations basées sur les notes
recommendations_data = []

for user in users_data:
    user_id = user["_id"]
    user_genres = user["preferences"]["genres"]
    
    # Trouver des films qui correspondent aux préférences de genre
    genre_matches = []
    for movie in movies_data:
        for genre in movie["genres"]:
            if genre in user_genres:
                genre_matches.append(movie)
                break
    
    # Filtrer les films que l'utilisateur a déjà évalués
    rated_movie_ids = [r["movie_id"] for r in ratings_data if r["user_id"] == user_id]
    recommendations = [m for m in genre_matches if m["_id"] not in rated_movie_ids]
    
    # Trier par popularité et prendre les 10 premiers
    recommendations.sort(key=lambda x: x["popularity"], reverse=True)
    recommendations = recommendations[:10]
    
    # Ajouter à la liste de recommandations
    for i, movie in enumerate(recommendations):
        # Calculer un score de recommandation
        score = round((movie["popularity"] * 0.5 + random.uniform(3, 5)) / 2, 1)
        recommendations_data.append({
            "user_id": user_id,
            "movie_id": movie["_id"],
            "title": movie["title"],
            "predicted_rating": score,
            "score": score,
            "genres": movie["genres"],
            "year": movie["year"],
            "rank": i + 1
        })

# Créer la collection recommendations et insérer les données
recommendations_collection = db["recommendations"]
recommendations_collection.insert_many(recommendations_data)
print(f"Collection 'recommendations' créée avec {recommendations_collection.count_documents({})} recommandations")

# Afficher un résumé des données créées
print("\nRésumé de la base de données 'movies_db':")
print(f"Collections: {db.list_collection_names()}")
for collection_name in db.list_collection_names():
    print(f" - {collection_name}: {db[collection_name].count_documents({})} documents")

# Afficher un exemple de chaque collection
print("\nExemples de documents:")
for collection_name in db.list_collection_names():
    sample = db[collection_name].find_one()
    print(f"\n{collection_name}:")
    # Convertir ObjectId en string pour l'affichage
    sample_str = {k: str(v) if isinstance(v, ObjectId) else v for k, v in sample.items()}
    print(json.dumps(sample_str, indent=2, default=str))

print("\nBase de données de test créée avec succès!")