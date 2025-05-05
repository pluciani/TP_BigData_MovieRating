# /app/app.py
import streamlit as st
import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://mongodb:27017/")
db = client["reco_db"]
collection = db["predictions"]

data = list(collection.find())

st.title("Données MongoDB")
if data:
    df = pd.DataFrame(data)
    st.dataframe(df)
else:
    st.warning("Aucune donnée trouvée dans la collection.")