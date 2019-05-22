#!/bin/sh

set -x
set -e

SPARK_URL=$1
TARGET_DIR=$2


mkdir -p /opt
wget -nv -O /opt/spark.tgz "$SPARK_URL"
tar xzf /opt/spark.tgz -C /opt/
rm /opt/spark.tgz

SPARK_DIR=$(basename $SPARK_URL .tgz)
ln -s "$SPARK_DIR" "$TARGET_DIR"
