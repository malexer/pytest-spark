import findspark

from .config import SparkConfigBuilder
from .fixtures import spark_context, spark_session, _spark_session
from .home import SparkHome


__all__ = (
    'spark_context',
    'spark_session',
)


def pytest_addoption(parser):
    parser.addini('spark_home', help='Spark install directory (SPARK_HOME).')
    parser.addoption(
        '--spark_home',
        dest='spark_home',
        help='Spark install directory (SPARK_HOME).',
    )

    parser.addini(
        'spark_options', help='Additional options for Spark.', type='linelist')


def pytest_configure(config):
    spark_home = SparkHome(config).path

    if spark_home:
        findspark.init(spark_home)

    spark_options = config.getini('spark_options')
    if spark_options:
        SparkConfigBuilder().initialize(options_from_ini=spark_options)


def pytest_report_header(config, startdir):
    header_lines = []
    spark_ver = SparkHome(config).version
    if spark_ver:
        spark_ver = spark_ver.strip().replace('\n', ' | ')
        header_lines.append('spark version -- ' + spark_ver)

    spark_options = SparkConfigBuilder().options
    if spark_options:
        header_lines.append('Spark will be initialized with options:')
        for k in sorted(spark_options.keys()):
            header_lines.append('  %s: %s' % (k, spark_options[k]))

    return '\n'.join(header_lines)
