# Sadalīta Spark klastera izveide ar HDFS un JupyterLab (10 TB datu apstrādei)

## Satura rādītājs  
- [1. Klastera specifikācija](#1-klastera-specifikācija)  
- [2. Nepieciešamā infrastruktūra](#2-nepieciešamā-infrastruktūra)  
- [3. Izvietošanas instrukcija](#3-izvietošanas-instrukcija)  
- [4. Tehnoloģiju komplekts](#4-tehnoloģiju-komplekts) 

## 1. Klastera specifikācija

### Galvenie komponenti
- **Master mezgls** (1 instances):
  - Spark Driver
  - Resource Manager
  - Klastera vadība

- **Darba mezgli** (3 instances):
  - Spark Executors
  - Datu apstrāde

### HDFS konfigurācija
- **NameNode** (1 instances):
  - Failu sistēmas vadība
- **DataNodes** (3 instances):
  - Datu glabāšana (katram 4+ TB)

### Papildus servisi
- **JupyterLab** (1 instances):
  - Datu analīze
  - PySpark izstrāde

## 2. Nepieciešamā infrastruktūra

### Serveru prasības
| Komponents | Procesors | Atmiņa | Krātuve | OS |
|------------|-----------|--------|---------|----|
| Master | 8+ kodoli | 16+ GB | 100+ GB | Ubuntu 22.04 |
| Workers | 16+ kodoli | 32+ GB | 4+ TB | Ubuntu 22.04 |

### Programmatūras prasības

# Visiem mezgliem
```sudo apt update
sudo apt install -y docker.io docker-compose
sudo systemctl enable docker
sudo usermod -aG docker $USER
```
## 3. Izvietošanas instrukcija  
### 3.1 Docker konfigurācija  
Izveidojiet docker-compose.yml datni:  
```
version: '3.8'
services:
  # Spark Master
  spark-master:
    image: bitnami/spark:3.5
    hostname: spark-master
    ports:
      - "8080:8080"  # Spark UI
      - "7077:7077"  # Spark Master
    environment:
      - SPARK_MODE=master
    volumes:
      - shared_data:/data
    networks:
      - spark-network

  # Spark Workers (3 instances)
  spark-worker-1:
    image: bitnami/spark:3.5
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=8
      - SPARK_WORKER_MEMORY=16g
    volumes:
      - shared_data:/data
    networks:
      - spark-network

  spark-worker-2:
    image: bitnami/spark:3.5
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=8
      - SPARK_WORKER_MEMORY=16g
    volumes:
      - shared_data:/data
    networks:
      - spark-network

  spark-worker-3:
    image: bitnami/spark:3.5
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=8
      - SPARK_WORKER_MEMORY=16g
    volumes:
      - shared_data:/data
    networks:
      - spark-network

  # HDFS NameNode
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    hostname: namenode
    ports:
      - "9870:9870"  # HDFS UI
      - "9000:9000"  # HDFS port
    volumes:
      - namenode_data:/hadoop/dfs/name
    environment:
      - CLUSTER_NAME=hadoop-cluster
    networks:
      - spark-network

  # HDFS DataNodes (3 instances)
  datanode1:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    depends_on:
      - namenode
    volumes:
      - datanode1_data:/hadoop/dfs/data
    environment:
      - SERVICE_PRECONDITION=namenode:9000
    networks:
      - spark-network

  datanode2:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    depends_on:
      - namenode
    volumes:
      - datanode2_data:/hadoop/dfs/data
    environment:
      - SERVICE_PRECONDITION=namenode:9000
    networks:
      - spark-network

  datanode3:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    depends_on:
      - namenode
    volumes:
      - datanode3_data:/hadoop/dfs/data
    environment:
      - SERVICE_PRECONDITION=namenode:9000
    networks:
      - spark-network

  # JupyterLab (PySpark)
  jupyter:
    image: jupyter/pyspark-notebook:latest
    ports:
      - "8888:8888"  # Jupyter UI
    volumes:
      - ./notebooks:/home/jovyan/work
      - shared_data:/data
    environment:
      - JUPYTER_TOKEN=parole123
    depends_on:
      - spark-master
      - namenode
    networks:
      - spark-network

volumes:
  namenode_data:
  datanode1_data:
  datanode2_data:
  datanode3_data:
  shared_data:

networks:
  spark-network:
    driver: bridge
```
	
### 3.2 Klastera palaišana  
```docker-compose up -d  
docker-compose ps
```

***Pārbauda, vai viss darbojas:***
```Spark UI: http://localhost:8080  
HDFS UI: http://localhost:9870  
Jupyter Lab: http://localhost:8888 (ar token parole123)  
```

### 3.3 Datu ievietošana HDFS 
```docker exec -it namenode bash  
hdfs dfs -mkdir /data  
hdfs dfs -put /local/data /data/
```

Pārbauda HDFS:
```hdfs dfs -ls /data  
hdfs dfs -du -h /data
```

### 3.4 PySpark darbība Jupyter Lab
Atver Jupyter Lab (http://localhost:8888)  
```from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Mans_Spark_uzdevums") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()
df = spark.read.parquet("hdfs://namenode:9000/data/datne.parquet")
df.show()
```

## 4. Tehnoloģiju komplekts 
Apache Spark	<img src="https://spark.apache.org/images/spark-logo-trademark.png" width="20">	3.5	Datu apstrāde  
Hadoop HDFS	<img src="https://hadoop.apache.org/images/hadoop-logo.jpg" width="20">	Datu glabāšana  
JupyterLab	<img src="https://jupyter.org/assets/homepage/main-logo.svg" width="20"> Analīzes vide  
Docker	<img src="https://www.docker.com/wp-content/uploads/2022/03/vertical-logo-monochromatic.png" width="20"> Konteinerizācija   
