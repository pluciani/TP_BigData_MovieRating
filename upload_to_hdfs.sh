#!/bin/bash

echo "📂 Vérification des fichiers dans /data :"
ls /data

echo "🚀 Création du dossier /data dans HDFS..."
hdfs dfs -mkdir -p /data

echo "📥 Upload des fichiers CSV dans HDFS..."
hdfs dfs -put -f /data/*.csv /data/

echo "✅ Upload terminé. Contenu de /data dans HDFS :"
hdfs dfs -ls /data