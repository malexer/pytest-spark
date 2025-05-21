import pytest

from .config import SparkConfigBuilder
from .util import reduce_logging


@pytest.fixture(scope='session')
def _spark_session():
    """Internal fixture for SparkSession instance.

    Yields SparkSession instance if it is supported by the pyspark
    version, otherwise yields None.

    Required to correctly initialize `spark_context` fixture after
    `spark_session` fixture.

    ..note::
        It is not possible to create SparkSession from the existing
        SparkContext.
    """

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        yield
    else:
        builder = SparkSession.builder \
            .config(conf=SparkConfigBuilder().get())
        if SparkConfigBuilder.is_spark_connect():
            builder = builder.remote(SparkConfigBuilder.spark_connect_url)
        session = builder.getOrCreate()

        yield session
        session.stop()


@pytest.fixture(scope='session')
def spark_context(_spark_session):
    """Return a SparkContext instance with reduced logging
    (session scope).
    """

    if SparkConfigBuilder.is_spark_connect():
        raise NotImplemented("Spark Connect doesn't support RDD API!")

    if _spark_session is None:
        from pyspark import SparkContext

        # pyspark 1.x: create SparkContext instance
        sc = SparkContext(conf=SparkConfigBuilder().get())
    else:
        # pyspark 2.x: get SparkContext from SparkSession fixture
        sc = _spark_session.sparkContext

    reduce_logging(sc)
    yield sc

    if _spark_session is None:
        sc.stop()  # pyspark 1.x: stop SparkContext instance


@pytest.fixture(scope='session')
def spark_session(_spark_session):
    """Return a Hive enabled SparkSession instance with reduced logging
    (session scope).

    Available from Spark 2.0 onwards.
    """

    if _spark_session is None:
        raise Exception(
            'The "spark_session" fixture is only available on spark 2.0 '
            'and above. Please use the spark_context fixture and instanciate '
            'a SQLContext or HiveContext from it in your tests.'
        )
    else:
        if not SparkConfigBuilder.is_spark_connect():
            reduce_logging(_spark_session.sparkContext)
        yield _spark_session
