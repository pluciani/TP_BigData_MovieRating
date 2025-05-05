import streamlit as st
import pymongo
import pandas as pd
import os

# Configuration de la page Streamlit
st.set_page_config(
    page_title="Recommandations de Films",
    page_icon="🎬",
    layout="wide"
)

# Titre de l'application
st.title("🎬 Recommandations de Films")
st.markdown("Cette application affiche les recommandations de films stockées dans MongoDB.")

# Connexion à MongoDB
@st.cache_resource
def get_mongo_client():
    """Établit la connexion avec MongoDB."""
    # Utilisez la variable d'environnement MONGO_URI si elle existe, sinon utilisez localhost
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    try:
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # Vérifiez que la connexion fonctionne
        client.server_info()
        return client
    except pymongo.errors.ServerSelectionTimeoutError as e:
        st.error(f"Erreur de connexion à MongoDB: {e}")
        return None

# Fonction pour récupérer les films avec leurs recommandations
@st.cache_data(ttl=60)  # Cache de 60 secondes
def get_recommendations(user_id=None, limit=100):
    """Récupère les recommandations de films depuis MongoDB."""
    client = get_mongo_client()
    if not client:
        return pd.DataFrame()
    
    try:
        db = client["movies_db"]  # Remplacer par le nom de votre base de données
        
        # Si vous avez une collection spécifique pour les recommandations
        if "recommendations" in db.list_collection_names():
            collection = db["recommendations"]
            query = {"user_id": user_id} if user_id else {}
            recommendations = list(collection.find(query).limit(limit))
            
            # Conversion en DataFrame pandas
            if recommendations:
                return pd.DataFrame(recommendations)
            else:
                return pd.DataFrame()
        
        # Si vous stockez les recommandations avec les films
        elif "movies" in db.list_collection_names():
            collection = db["movies"]
            movies = list(collection.find({}).limit(limit))
            
            if movies:
                return pd.DataFrame(movies)
            else:
                return pd.DataFrame()
        
        # Si vous avez une autre structure
        else:
            st.warning("Structure de base de données non reconnue.")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Erreur lors de la récupération des recommandations: {e}")
        return pd.DataFrame()

# Interface utilisateur
def main():
    # Sidebar pour les filtres
    st.sidebar.header("Filtres")
    
    # Connection status
    client = get_mongo_client()
    if client:
        st.sidebar.success("✅ Connecté à MongoDB")
    else:
        st.sidebar.error("❌ Non connecté à MongoDB")
        st.stop()
    
    # Récupérer la liste des utilisateurs si nécessaire
    try:
        db = client["movies_db"]
        if "recommendations" in db.list_collection_names():
            users = db["recommendations"].distinct("user_id")
        elif "users" in db.list_collection_names():
            users = db["users"].distinct("_id")
        elif "ratings" in db.list_collection_names():
            users = db["ratings"].distinct("user_id")
        else:
            users = []
    except:
        users = []
    
    # Sélection de l'utilisateur si des utilisateurs existent
    user_id = None
    if users:
        user_id = st.sidebar.selectbox(
            "Sélectionner un utilisateur",
            ["Tous les utilisateurs"] + users
        )
        if user_id == "Tous les utilisateurs":
            user_id = None
    
    # Nombre de recommandations à afficher
    num_recs = st.sidebar.slider(
        "Nombre de recommandations", 
        min_value=5, 
        max_value=100, 
        value=20
    )
    
    # Filtre par note minimale (si applicable)
    min_rating = st.sidebar.slider(
        "Note minimale", 
        min_value=0.0, 
        max_value=5.0, 
        value=0.0, 
        step=0.5
    )
    
    # Récupérer les recommandations
    recommendations_df = get_recommendations(user_id, limit=num_recs)
    
    # Afficher les résultats
    if not recommendations_df.empty:
        # Déterminer les colonnes à afficher en fonction de ce qui est disponible
        display_cols = []
        
        # Colonnes courantes pour l'affichage
        if "title" in recommendations_df.columns:
            display_cols.append("title")
        elif "movie_title" in recommendations_df.columns:
            display_cols.append("movie_title")
        
        if "rating" in recommendations_df.columns:
            display_cols.append("rating")
        elif "predicted_rating" in recommendations_df.columns:
            display_cols.append("predicted_rating")
        elif "score" in recommendations_df.columns:
            display_cols.append("score")
        
        if "genres" in recommendations_df.columns:
            display_cols.append("genres")
        
        if "year" in recommendations_df.columns:
            display_cols.append("year")
        
        # Filtrer par note minimale si applicable
        rating_col = None
        for col in ["rating", "predicted_rating", "score"]:
            if col in recommendations_df.columns:
                rating_col = col
                break
        
        if rating_col:
            filtered_df = recommendations_df[recommendations_df[rating_col] >= min_rating]
        else:
            filtered_df = recommendations_df
        
        # Afficher le nombre de recommandations
        st.write(f"**{len(filtered_df)}** recommandations trouvées")
        
        # Afficher le tableau de recommandations
        if not display_cols:  # Si nous ne savons pas quelles colonnes afficher
            st.dataframe(filtered_df)
        else:
            # Sélectionner et renommer les colonnes pour l'affichage
            display_df = filtered_df[display_cols].copy()
            
            # Renommer les colonnes pour un affichage plus convivial
            column_mapping = {
                "title": "Titre",
                "movie_title": "Titre",
                "rating": "Note",
                "predicted_rating": "Note prédite",
                "score": "Score",
                "genres": "Genres",
                "year": "Année"
            }
            
            display_df = display_df.rename(columns={col: column_mapping.get(col, col) for col in display_cols})
            
            # Afficher le tableau
            st.dataframe(display_df, use_container_width=True)
            
            # Visualisation des données si suffisamment de données sont disponibles
            if rating_col and len(filtered_df) >= 5:
                st.subheader("Distribution des notes")
                
                # Histogramme des notes
                hist_values = filtered_df[rating_col].value_counts().sort_index()
                st.bar_chart(hist_values)
    else:
        st.info("Aucune recommandation trouvée. Veuillez vérifier votre connexion MongoDB et la structure de vos données.")
        
        # Afficher des informations de débogage
        st.subheader("Informations de débogage")
        
        try:
            client = get_mongo_client()
            if client:
                db = client["movies_db"]
                collections = db.list_collection_names()
                
                st.write("Collections disponibles dans la base de données:")
                for collection in collections:
                    count = db[collection].count_documents({})
                    st.write(f"- {collection}: {count} documents")
                
                # Montrer un exemple de document pour chaque collection
                st.write("Exemples de documents:")
                for collection in collections:
                    sample = db[collection].find_one()
                    if sample:
                        st.write(f"**{collection}**:")
                        # Convertir les ObjectId en string pour l'affichage
                        sample_str = {k: str(v) if isinstance(v, pymongo.objectid.ObjectId) else v for k, v in sample.items()}
                        st.json(sample_str)
        except Exception as e:
            st.error(f"Erreur lors de la récupération des informations de débogage: {e}")

if __name__ == "__main__":
    main()