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
ENV SPARK24_URL spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
RUN export APACHE_MIRROR=$(curl -s 'https://www.apache.org/dyn/closer.cgi?as_json=1' | python -c "import sys, json; print json.load(sys.stdin)['preferred']") \
    && export SPARK24_FULL_URL="${APACHE_MIRROR}${SPARK24_URL}" \
    && /download_spark.sh $SPARK24_FULL_URL /opt/spark24

# prepare to run tests
RUN pip install --no-cache-dir tox
COPY ./ ./test_env/tox.ini /tests/

CMD [ "tox" ]
