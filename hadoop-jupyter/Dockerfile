FROM ubuntu:focal

ARG ARCH=amd64
ARG DEBIAN_FRONTEND=noninteractive

ENV ARCH=${ARCH}
ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-${ARCH}/jre"
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV SPARK_HOME=/opt/spark
ENV HIVE_HOME=/opt/hive
ENV TEZ_HOME=/opt/tez
ENV PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${HIVE_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.7-src.zip"
ENV CLASSPATH="${HADOOP_HOME}/lib/*:${HIVE_HOME}/lib/*:${TEZ_HOME}/lib/*"
ENV HADOOP_VERSION=2.7.0
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS='notebook'
ENV PYSPARK_PYTHON=python3

RUN apt-get update && \
    apt-get install -y wget nano openjdk-8-jdk ssh openssh-server \
    python3 python3-pip python3-dev build-essential libssl-dev libffi-dev libpq-dev libjpeg-dev pkg-config

COPY /confs/requirements.req /
RUN pip3 install --upgrade pip setuptools wheel && \
    pip3 install cython numpy matplotlib
RUN pip3 install grpcio>=1.49.0 && \
    pip3 install -r requirements.req && \
    pip3 install "dask[bag]" --upgrade
RUN pip3 install --upgrade toree && \
    pip3 install bash_kernel && \
    python3 -m bash_kernel.install

RUN wget -P /tmp/ https://archive.apache.org/dist/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz && \
    tar xvf /tmp/hadoop-2.7.0.tar.gz -C /tmp && \
    mv /tmp/hadoop-2.7.0 ${HADOOP_HOME}

RUN wget -P /tmp/ https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz && \
    tar xvf /tmp/spark-2.4.8-bin-hadoop2.7.tgz -C /tmp && \
    mv /tmp/spark-2.4.8-bin-hadoop2.7 ${SPARK_HOME}

COPY patch_cloudpickle.py /tmp/patch_cloudpickle.py
RUN python3 /tmp/patch_cloudpickle.py

RUN wget -P /tmp/ https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz && \
    tar xvf /tmp/apache-hive-2.3.9-bin.tar.gz -C /tmp && \
    mv /tmp/apache-hive-2.3.9-bin ${HIVE_HOME}

RUN wget -P /tmp/ https://archive.apache.org/dist/tez/0.10.2/apache-tez-0.10.2-bin.tar.gz && \
    tar xvf /tmp/apache-tez-0.10.2-bin.tar.gz -C /tmp && \
    mv /tmp/apache-tez-0.10.2-bin ${TEZ_HOME}

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 600 ~/.ssh/authorized_keys
COPY /confs/config /root/.ssh
RUN chmod 600 /root/.ssh/config

COPY /confs/*.xml /opt/hadoop/etc/hadoop/
COPY /confs/slaves /opt/hadoop/etc/hadoop/
COPY bootstrap.sh /
COPY /confs/spark-defaults.conf ${SPARK_HOME}/conf
COPY /confs/hive-site.xml ${HIVE_HOME}/conf

RUN jupyter toree install --spark_home=${SPARK_HOME}
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/environment

RUN mkdir lab

ENTRYPOINT ["/bin/bash", "bootstrap.sh"]
