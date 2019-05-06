import pyspark
import pyspark.sql
import pytest


try:
    from pyspark.sql import SparkSession
except ImportError:
    SPARK1 = True
else:
    SPARK1 = False


@pytest.mark.skipif(SPARK1, reason="requires Spark 2.x")
def test_spark_session_dataframe(spark_session):
    test_df = spark_session.createDataFrame([[1, 3], [2, 4]], "a: int, b: int")

    assert type(test_df) == pyspark.sql.dataframe.DataFrame
    assert test_df.count() == 2


@pytest.mark.skipif(SPARK1, reason="requires Spark 2.x")
def test_spark_session_sql(spark_session):
    test_df = spark_session.createDataFrame([[1, 3], [2, 4]], "a: int, b: int")
    test_df.registerTempTable('test')

    test_filtered_df = spark_session.sql('SELECT a, b from test where a > 1')
    assert test_filtered_df.count() == 1
