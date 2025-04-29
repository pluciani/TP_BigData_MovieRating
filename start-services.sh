#!/bin/bash

# Configuration du PATH explicitement à l'intérieur du script
export PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:/opt/spark/sbin:/opt/kafka/bin:/usr/local/bin:/usr/bin:/bin

# Format HDFS namenode
/opt/hadoop/bin/hdfs namenode -format

# Start Hadoop services
/opt/hadoop/sbin/start-dfs.sh
/opt/hadoop/sbin/start-yarn.sh

# Start ZooKeeper
/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &

# Wait for ZooKeeper to start
/bin/sleep 5

# Start Kafka
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &

# Start Jupyter
/usr/local/bin/jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --NotebookApp.token='' --NotebookApp.password=''