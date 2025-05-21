# Bāzes attēls
FROM ubuntu:22.04

# Spark un Hadoop vides mainīgie
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV SPARK_VERSION="3.4.2"
ENV HADOOP_VERSION="3"
ENV HADOOP_CLI_VERSION="3.3.6"
ENV SPARK_HOME="/opt/spark"
ENV HADOOP_HOME="/opt/hadoop"
ENV VIRTUAL_ENV=/opt/venv

# PATH tiek atjaunināts pakāpeniski, lai nodrošinātu pareizu secību
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PATH="$SPARK_HOME/sbin:$PATH"
ENV PATH="$HADOOP_HOME/bin:$PATH"

# Spark pakotnes, kas atbilst Spark Scala versijai (2.12)
ENV SPARK_PACKAGES="io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-avro_2.12:${SPARK_VERSION}"
ENV PYSPARK_SUBMIT_ARGS="--packages ${SPARK_PACKAGES} pyspark-shell"

USER root

# Atjaunina un instalē nepieciešamās pakotnes, ieskaitot Python 3.10
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    wget tar openjdk-11-jdk-headless python3.10 python3.10-venv python3-pip \
    locales tree graphviz graphviz-dev dialog apt-utils && \
    rm -rf /var/lib/apt/lists/*

# Lokālais iestatījums
RUN locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

# Virtuālās Python vides izveide, izmantojot python3.10
RUN python3.10 -m venv $VIRTUAL_ENV
ENV PYSPARK_PYTHON="${VIRTUAL_ENV}/bin/python"

# Pip un bibliotēku instalācija
RUN ${VIRTUAL_ENV}/bin/pip install --no-cache-dir --upgrade pip && \
    ${VIRTUAL_ENV}/bin/pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    delta-spark==2.4.0 \
    numpy pandas scipy seaborn matplotlib scikit-learn \
    pyarrow fastparquet polars jsonlines \
    rdflib lxml great-expectations ydata-profiling \
    psycopg2-binary python-dotenv openpyxl ipywidgets

# Lejupielādē un instalē Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -O /tmp/spark.tgz && \
    tar -xvzf /tmp/spark.tgz -C /opt && \
    rm /tmp/spark.tgz && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Lejupielādē Hadoop CLI
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_CLI_VERSION}/hadoop-${HADOOP_CLI_VERSION}.tar.gz -O /tmp/hadoop.tar.gz && \
    tar -xvzf /tmp/hadoop.tar.gz -C /opt && \
    rm /tmp/hadoop.tar.gz && \
    ln -s /opt/hadoop-${HADOOP_CLI_VERSION} ${HADOOP_HOME}

ENV HADOOP_CONF_DIR=${SPARK_HOME}/conf
ENV LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH}"

RUN cp ${SPARK_HOME}/conf/spark-defaults.conf.template ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog" >> ${SPARK_HOME}/conf/spark-defaults.conf

# Noklusējuma darba mape
ARG shared_workspace=/opt/workspace
RUN mkdir -p ${shared_workspace}
WORKDIR ${shared_workspace}
VOLUME ${shared_workspace}

# Noklusējuma startēšana Spark darbiniekam
# Šī komanda nodrošinās, ka konteiners paliek aktīvs un palaiž Spark darbinieku.
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]