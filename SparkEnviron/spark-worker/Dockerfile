FROM bde2020/spark-worker:3.0.1-hadoop3.2
#COPY ./jars/ ./spark/jars
COPY ./hive-site.xml /spark/conf/
ENV PYSPARK_PYTHON=python3
RUN export PYSPARK_PYTHON