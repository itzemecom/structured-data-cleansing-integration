#!/bin/bash

# Definē versijas (kā jau ir jūsu failā)
SPARK_VERSION="3.4.2"
HADOOP_VERSION="3.2"
JUPYTERLAB_VERSION="3.1.0"

# -- Izveido attēlus (esošās komandas)
docker build \
-f cluster-base.Dockerfile \
-t cluster-base .

docker build \
--build-arg spark_version="${SPARK_VERSION}" \
--build-arg hadoop_version="${HADOOP_VERSION}" \
-f spark-base.Dockerfile \
-t spark-base .

docker build \
-f spark-master.Dockerfile \
-t spark-master .

docker build \
-f spark-worker.Dockerfile \
-t spark-worker .

docker build \
--build-arg spark_version="${SPARK_VERSION}" \
--build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
-f jupyterlab.Dockerfile \
-t jupyterlab .

# --- Pievienot: Palaist Docker Compose servisus ---
echo "Docker attēlu veidošana pabeigta. Notiek Docker Composite pakalpojumu palaišana..."
docker compose up -d
echo "Docker Compose pakalpojumi tika palaisti atvienotā režīmā."
# --- Pievienot beigas ---