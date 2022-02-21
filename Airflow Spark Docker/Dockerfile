FROM python:3.7.7-stretch

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=2.1.3
# 1.10.15
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS="pyspark==3.0.1 SQLAlchemy==1.3.15"
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
RUN export AIRFLOW_HOME
# Disable noisy "Handling signal" log messages:
ENV GUNICORN_CMD_ARGS --log-level WARNING
ENV AIRFLOW_CONN_SPARK_DEFAULT=spark://spark-master:7077
RUN export AIRFLOW_CONN_SPARK_DEFAULT
RUN set -ex \
    && buildDeps=' \
        apt-utils \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        iputils-ping \
        telnet \
    && locale-gen \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install --use-feature=2020-resolver apache-airflow[hive,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && pip install 'redis>=2.10.5,<3' \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base \
    && python --version \
    && pip freeze

RUN pip install apache-airflow-providers-apache-spark
###############################
## Begin JAVA installation
###############################
# Java is required in order to spark-submit work
# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \ 
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    pip freeze && \
    java -version $$ \
    javac -version
#
## Setup JAVA_HOME 
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME
################################
### Finish JAVA installation
################################
#
COPY entrypoint.sh /entrypoint.sh
#COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow/airflow.cfg
#
################################
### SPARK files and variables
################################
## Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
COPY spark_files/spark-3.0.1-bin-hadoop3.2/bin /usr/local/spark/bin 
COPY spark_files/spark-3.0.1-bin-hadoop3.2/jars /usr/local/spark/assembly/target/scala-2.12/jars
#COPY ./jars/ /usr/local/spark/assembly/target/scala-2.12/jars/
## Create SPARK_HOME env var
ENV SPARK_HOME /usr/local/spark
RUN export SPARK_HOME
#ENV PATH $PATH:/usr/local/spark/bin
################################
### Finish SPARK files and variables
################################

RUN usermod -a -G sudo airflow
##

#
RUN chown -R airflow: ${AIRFLOW_HOME}
RUN mkdir -p /opt/workspace/
RUN chown -R airflow: /opt/workspace/
RUN chmod +x entrypoint.sh
RUN chmod +rwx /opt/workspace/
RUN chmod +rwx /usr/local/
RUN chmod +rwx /usr/local/bin/
RUN chmod -R 777 /usr/local/spark/bin/

RUN airflow db init
RUN chmod 777 /usr/local/airflow/airflow.db
RUN chown -R airflow: /usr/local/airflow/airflow.db
RUN chmod -R 777 /usr/local/airflow/logs/
# RUN chmod 777 /usr/local/airflow/logs/scheduler/

EXPOSE 8080 5555 8793
RUN airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
COPY ./hive-site.xml /usr/local/spark
USER airflow
ENV PYSPARK_PYTHON=python3
RUN export PYSPARK_PYTHON
WORKDIR /opt/workspace/
ENTRYPOINT ["/entrypoint.sh"]
# set default arg for entrypoint
CMD ["webserver"]
