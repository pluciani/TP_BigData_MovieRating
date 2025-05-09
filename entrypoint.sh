#!/bin/bash
set -e

# Load Java environment
source /etc/profile.d/java.sh

# Export Hadoop and Kafka paths
export HADOOP_HOME=/opt/hadoop
export KAFKA_HOME=/opt/kafka
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$KAFKA_HOME/bin

# Debugging: Show PATH
echo "PATH = $PATH"
echo "JAVA_HOME = $JAVA_HOME"
echo "HADOOP_HOME = $HADOOP_HOME"
echo "KAFKA_HOME = $KAFKA_HOME"

# Start SSH service
echo "Starting SSH service..."
/usr/sbin/sshd

# Setup passwordless SSH
if [ ! -f ~/.ssh/id_rsa ]; then
  echo "Setting up passwordless SSH..."
  mkdir -p ~/.ssh
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  chmod 600 ~/.ssh/authorized_keys
fi

# Configure SSH known hosts
mkdir -p ~/.ssh
echo -e "Host localhost\n   StrictHostKeyChecking no\nHost namenode\n   StrictHostKeyChecking no" >> ~/.ssh/config
chmod 600 ~/.ssh/config

# Format Hadoop filesystem if needed
if [ ! -d /opt/hadoop_data/hdfs/namenode/current ]; then
  echo "Formatting NameNode..."
  hdfs namenode -format -force -nonInteractive
fi

# Start Hadoop services
echo "Starting HDFS (NameNode and DataNode)..."
$HADOOP_HOME/sbin/start-dfs.sh

echo "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

echo "Starting MapReduce HistoryServer..."
$HADOOP_HOME/bin/mapred --daemon start historyserver

# Start ZooKeeper and Kafka
echo "Starting ZooKeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties

# Wait for Zookeeper to be ready (without using nc)
echo "Waiting for Zookeeper to be ready..."
sleep 10  # Wait 10 seconds

timeout=30
counter=0
zk_ready=false
while [ $counter -lt $timeout ] && [ "$zk_ready" = false ]; do
  if echo stat | timeout 1 bash -c "cat > /dev/tcp/localhost/2181"; then
    echo "ZooKeeper is ready!"
    zk_ready=true
  else
    echo "Waiting for ZooKeeper... ($counter/$timeout)"
    counter=$((counter+1))
    sleep 1
  fi
done

if [ "$zk_ready" = false ]; then
  echo "Warning: ZooKeeper may not be ready after $timeout seconds, but continuing anyway..."
fi

echo "Starting Kafka broker..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

# Start Jupyter Notebook
echo "Starting Jupyter Notebook..."
mkdir -p /notebooks
jupyter notebook \
  --ip=0.0.0.0 \
  --port=8888 \
  --allow-root \
  --NotebookApp.token='' \
  --NotebookApp.password='' \
  --notebook-dir=/notebooks \
  --no-browser &


# Upload automatique vers HDFS
/upload_to_hdfs.sh

# Démarrer Streamlit en arrière-plan
if [ -f "/app/app.py" ]; then
  echo "Démarrage de l'application Streamlit..."
  nohup /start_streamlit.sh > /var/log/streamlit.log 2>&1 &
else
  echo "Aucun fichier app.py trouvé dans /app, Streamlit ne sera pas démarré."
fi

# Keep container alive
tail -f /dev/null