import os

import findspark
import pytest


def find_spark_home_var(config):
    spark_home = config.getini('spark_home')
    source = 'config (pytest.ini)'

    if not spark_home:
        spark_home = os.environ.get('SPARK_HOME')
        source = 'ENV'

    if not spark_home:
        raise ValueError(
            '"SPARK_HOME" variable was not found neither in pytest.ini '
            'nor as environmental variable.')

    return spark_home, source


def update_spark_home(spark_home, source):
    """Set SPARK_HOME with provided path and perform all required
    configuration to make pyspark importable.

    :param spark_home: path to Apache Spark
    :param source: name of the source, used for error message in case if
                   specified path was not found
    """

    if spark_home is not None:
        spark_home = os.path.abspath(spark_home)
        if not os.path.exists(spark_home):
            raise OSError(
                "SPARK_HOME path specified in %s does not exist: %s"
                % (source, spark_home))

    findspark.init(spark_home)


def get_spark_version(spark_home):
    if spark_home:
        spark_home = os.path.abspath(spark_home)
        release_info_filename = os.path.join(spark_home, 'RELEASE')
        if os.path.exists(release_info_filename):
            with open(release_info_filename) as release_info:
                return release_info.read()


def pytest_addoption(parser):
    parser.addini('spark_home', 'Spark install directory (SPARK_HOME).')


def pytest_configure(config):
    spark_home, source = find_spark_home_var(config)

    if spark_home:
        update_spark_home(spark_home, source)


def pytest_report_header(config, startdir):
    spark_home, _ = find_spark_home_var(config)
    if spark_home:
        spark_ver = get_spark_version(spark_home)
        if spark_ver:
            spark_ver = spark_ver.strip().replace('\n', ' | ')
            return "spark version -- " + spark_ver


@pytest.fixture(scope='session')
def spark_context():
    """Return a SparkContext instance with reduced logging
    (session scope).
    """

    from pyspark import SparkContext

    sc = SparkContext()

    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.OFF)

    yield sc

    sc.stop()


@pytest.fixture(scope='session')
def spark_session():
    """Return a Hive enabled SparkSession instance with reduced logging
    (session scope).

    Available from Spark 2.0 onwards.
    """

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        raise Exception(
            'The "spark_session" fixture is only available on spark 2.0 '
            'and above. Please use the spark_context fixture and instanciate '
            'a SQLContext or HiveContext from it in your tests.'
        )

    spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
    sc = spark_session.sparkContext

    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.OFF)

    yield spark_session

    spark_session.stop()
