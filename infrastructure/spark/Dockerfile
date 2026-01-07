FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    default-jre \
    curl wget vim \
    openssh-server ssh \
    net-tools ca-certificates \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python \
    PYTHONHASHSEED=1

ENV PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"

RUN pip install --no-cache-dir \
    poetry pyspark==3.5.2 click==8.3.1

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mkdir -p /opt/spark \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/spark --strip-components=1 \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN mkdir -p /var/run/sshd \
    && echo 'root:password' | chpasswd \
    && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

EXPOSE 22 7077 8080

ENV SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
    SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out

RUN mkdir -p $SPARK_LOG_DIR \
    && touch $SPARK_MASTER_LOG $SPARK_WORKER_LOG \
    && ln -sf /dev/stdout $SPARK_MASTER_LOG \
    && ln -sf /dev/stdout $SPARK_WORKER_LOG

WORKDIR /opt/spark
COPY start-spark.sh .
RUN chmod +x start-spark.sh

CMD ["/bin/bash"]
