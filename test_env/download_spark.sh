#!/bin/sh

SPARK_URL=$1
TARGET_DIR=$2

set -e

mkdir -p /opt
wget -q -O /opt/spark.tgz "$SPARK_URL"
tar xzf /opt/spark.tgz -C /opt/
rm /opt/spark.tgz

SPARK_DIR=$(basename $SPARK_URL .tgz)
ln -s "$SPARK_DIR" "$TARGET_DIR"
