FROM jupyter/all-spark-notebook:5cfa60996e84
#COPY ./SparkEnviron/jars/ /usr/local/spark/jars/
#RUN conda install python=3.7.6 -y
ENV PATH="${PATH}:/opt/workspace"
