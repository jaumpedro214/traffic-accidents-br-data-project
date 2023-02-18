FROM docker.io/bitnami/spark:3.3.1

COPY *.jar $SPARK_HOME/jars

RUN pip install delta-spark