FROM bde2020/spark-master:3.0.1-hadoop3.2
#COPY ./jars/ ./spark/jars
COPY ./hive-site.xml /usr/local/spark/conf/
ENV PYSPARK_PYTHON=python3
RUN export PYSPARK_PYTHON