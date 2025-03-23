FROM openjdk:8-jdk-alpine

WORKDIR /tests/

# install packages
RUN apk --no-cache add bash curl wget py-pip python3

# install Spark download script
COPY ./test_env/download_spark.sh /
RUN chmod +x /download_spark.sh

# install Spark 1.6
ENV SPARK16_URL https://archive.apache.org/dist/spark/spark-1.6.3/spark-1.6.3-bin-hadoop2.6.tgz
RUN /download_spark.sh $SPARK16_URL /opt/spark16

# install Spark 2.4
ENV SPARK24_URL https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz
RUN /download_spark.sh $SPARK24_URL /opt/spark24

# install Spark 2.4
ENV SPARK35_URL https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
RUN /download_spark.sh $SPARK35_URL /opt/spark35

# prepare to run tests
RUN pip install --no-cache-dir tox
COPY ./ ./test_env/tox.ini /tests/

CMD [ "tox" ]
