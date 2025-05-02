#!/bin/bash

# Attendre que MongoDB soit prêt (si nécessaire)
echo "Vérification de la connexion à MongoDB..."
timeout=60
counter=0
while ! mongo --host mongodb --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
    sleep 1
    counter=$((counter + 1))
    if [ $counter -eq $timeout ]; then
        echo "Impossible de se connecter à MongoDB après $timeout secondes."
        exit 1
    fi
    echo "En attente de MongoDB... ($counter/$timeout)"
done

echo "MongoDB est prêt!"

# Lancer l'application Streamlit
cd /app
echo "Démarrage de l'application Streamlit..."
streamlit run /app/app.py --server.port=8501 --server.address=0.0.0.0