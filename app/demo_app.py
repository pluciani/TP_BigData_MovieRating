import streamlit as st
import pymongo
import pandas as pd
import os
from bson import ObjectId

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
def get_recommendations(user_id=None, min_prediction=0, limit=100):
    """Récupère les recommandations de films depuis MongoDB."""
    client = get_mongo_client()
    if not client:
        return pd.DataFrame()
    
    try:
        db = client["reco_db"]  # Utiliser la base de données "reco_db"
        collection = db["predictions"]  # Utiliser la collection "predictions"
        
        # Construire la requête en fonction des paramètres
        query = {}
        if user_id is not None:
            query["user"] = user_id
        
        # Ajouter un filtre sur la prédiction minimale
        if min_prediction > 0:
            query["prediction"] = {"$gte": min_prediction}
        
        # Récupérer les recommandations
        recommendations = list(collection.find(query).sort("prediction", -1).limit(limit))
        
        # Conversion en DataFrame pandas
        if recommendations:
            return pd.DataFrame(recommendations)
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Erreur lors de la récupération des recommandations: {e}")
        return pd.DataFrame()

# Fonction pour récupérer la liste des utilisateurs
@st.cache_data(ttl=300)  # Cache de 5 minutes
def get_users():
    """Récupère la liste des utilisateurs distincts."""
    client = get_mongo_client()
    if not client:
        return []
    
    try:
        db = client["reco_db"]
        collection = db["predictions"]
        users = collection.distinct("user")
        return sorted(users)
    except Exception as e:
        st.error(f"Erreur lors de la récupération des utilisateurs: {e}")
        return []

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
    
    # Récupérer la liste des utilisateurs
    users = get_users()
    
    # Sélection de l'utilisateur si des utilisateurs existent
    user_id = None
    if users:
        user_option = st.sidebar.selectbox(
            "Sélectionner un utilisateur",
            ["Tous les utilisateurs"] + [f"Utilisateur {user}" for user in users]
        )
        if user_option != "Tous les utilisateurs":
            user_id = int(user_option.split(" ")[1])
    
    # Nombre de recommandations à afficher
    num_recs = st.sidebar.slider(
        "Nombre de recommandations", 
        min_value=5, 
        max_value=100, 
        value=20
    )
    
    # Filtre par prédiction minimale
    min_prediction = st.sidebar.slider(
        "Prédiction minimale", 
        min_value=0.0, 
        max_value=5.0, 
        value=3.0, 
        step=0.1
    )
    
    # Récupérer les recommandations
    recommendations_df = get_recommendations(user_id, min_prediction, limit=num_recs)
    
    # Afficher les résultats
    if not recommendations_df.empty:
        # Renommer les colonnes pour l'affichage
        display_df = recommendations_df.copy()
        if "_id" in display_df.columns:
            display_df = display_df.drop(columns=["_id"])
        
        # Renommer les colonnes
        column_mapping = {
            "user": "Utilisateur",
            "movie": "ID Film",
            "title": "Titre",
            "prediction": "Score prédit"
        }
        display_df = display_df.rename(columns=column_mapping)
        
        # Arrondir le score prédit à 2 décimales
        if "Score prédit" in display_df.columns:
            display_df["Score prédit"] = display_df["Score prédit"].round(2)
        
        # Afficher le nombre de recommandations
        st.write(f"**{len(display_df)}** recommandations trouvées")
        
        # Afficher le tableau de recommandations
        st.dataframe(display_df, use_container_width=True)
        
        # Visualisation des données
        if len(display_df) >= 5 and "Score prédit" in display_df.columns:
            st.subheader("Distribution des scores prédits")
            
            # Créer des tranches pour l'histogramme
            display_df["Tranche de score"] = pd.cut(
                display_df["Score prédit"], 
                bins=[0, 1, 2, 3, 4, 5], 
                labels=["0-1", "1-2", "2-3", "3-4", "4-5"]
            )
            
            # Compter les occurrences dans chaque tranche
            score_counts = display_df["Tranche de score"].value_counts().sort_index()
            
            # Afficher l'histogramme
            st.bar_chart(score_counts)
            
            # Afficher des statistiques sur les scores
            st.subheader("Statistiques des scores")
            stats = {
                "Score moyen": display_df["Score prédit"].mean(),
                "Score minimum": display_df["Score prédit"].min(),
                "Score maximum": display_df["Score prédit"].max(),
                "Écart-type": display_df["Score prédit"].std()
            }
            stats_df = pd.DataFrame(stats.items(), columns=["Statistique", "Valeur"])
            stats_df["Valeur"] = stats_df["Valeur"].round(2)
            st.dataframe(stats_df, use_container_width=True)
    else:
        st.info("Aucune recommandation trouvée avec les critères sélectionnés.")
        
        # Afficher des informations de débogage
        st.subheader("Informations de débogage")
        
        try:
            client = get_mongo_client()
            if client:
                db = client["reco_db"]
                
                # Vérifier que la collection existe
                if "predictions" in db.list_collection_names():
                    count = db["predictions"].count_documents({})
                    st.write(f"Collection 'predictions': {count} documents")
                    
                    # Montrer un exemple de document
                    sample = db["predictions"].find_one()
                    if sample:
                        st.write("Exemple de document:")
                        # Convertir les ObjectId en string pour l'affichage
                        sample_str = {k: str(v) if isinstance(v, ObjectId) else v for k, v in sample.items()}
                        st.json(sample_str)
                else:
                    st.error("La collection 'predictions' n'existe pas dans la base de données 'reco_db'")
                    
                    # Afficher les collections disponibles
                    collections = db.list_collection_names()
                    if collections:
                        st.write("Collections disponibles:")
                        for coll in collections:
                            st.write(f"- {coll}")
                    else:
                        st.write("Aucune collection trouvée dans la base de données 'reco_db'")
        except Exception as e:
            st.error(f"Erreur lors de la récupération des informations de débogage: {e}")

if __name__ == "__main__":
    main()