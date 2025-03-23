import os


class SparkConfigBuilder(object):

    DEFAULTS = {
        'spark.app.name': 'pytest-spark',
        'spark.default.parallelism': 1,
        'spark.dynamicAllocation.enabled': 'false',
        'spark.executor.cores': 1,
        'spark.executor.instances': 1,
        'spark.io.compression.codec': 'lz4',
        'spark.rdd.compress': 'false',
        'spark.sql.shuffle.partitions': 1,
        'spark.shuffle.compress': 'false',
        'spark.sql.catalogImplementation': 'hive',
    }

    SPARK_REMOTE_DISABLED_SETTINGS = {
        'spark.sql.catalogImplementation',
        'spark.executor.cores',
        'spark.executor.instances',
        'spark.rdd.compress',
        'spark.sql.extensions',
        'spark.sql.catalog.spark_catalog',
        'spark.jars.packages',
        'spark.shuffle.compress',
        'spark.io.compression.codec',
        'spark.dynamicAllocation.enabled',
        # '',
        # '',
        # '',
        # '',
    }

    options = None
    _instance = None
    spark_connect_url = None

    @classmethod
    def _parse_config(cls, values):

        def parse_value_string(value_str):
            split_char = ':' if ':' in value_str else '='
            k, v = [s.strip() for s in value_str.split(split_char, 1)]
            return (k, v)

        return dict([parse_value_string(val) for val in values])

    @classmethod
    def initialize(cls, options_from_ini=None, spark_connect_url=None):

        if cls._instance:
            return cls._instance

        from pyspark import SparkConf

        cls._instance = SparkConf()

        cls.options = dict(cls.DEFAULTS)
        if options_from_ini:
            opts = cls._parse_config(options_from_ini)
            cls.options.update(opts)
        if spark_connect_url or os.environ.get("SPARK_REMOTE"):
            cls.spark_connect_url = spark_connect_url or os.environ.get("SPARK_REMOTE")
            for k in cls.SPARK_REMOTE_DISABLED_SETTINGS:
                if k in cls.options:
                    del cls.options[k]

        for k, v in cls.options.items():
            cls._instance.set(k, v)

        return cls._instance

    @classmethod
    def get(cls):
        if not cls._instance:
            cls.initialize()

        return cls._instance

    @classmethod
    def is_spark_connect(cls):
        if not cls._instance:
            cls.initialize()
        return bool(cls.spark_connect_url)