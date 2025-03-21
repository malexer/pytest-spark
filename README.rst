pytest-spark
############

pytest_ plugin to run the tests with support of pyspark (`Apache Spark`_).

This plugin will allow to specify SPARK_HOME directory in ``pytest.ini``
and thus to make "pyspark" importable in your tests which are executed
by pytest.

You can also define "spark_options" in ``pytest.ini`` to customize pyspark,
including "spark.jars.packages" option which allows to load external
libraries (e.g. "com.databricks:spark-xml").

pytest-spark provides session scope fixtures ``spark_context`` and
``spark_session`` which can be used in your tests.

**Note:** no need to define SPARK_HOME if you've installed pyspark using
pip (e.g. ``pip install pyspark``) - it should be already importable. In
this case just don't define SPARK_HOME neither in pytest
(pytest.ini / --spark_home) nor as environment variable.


Install
=======

.. code-block:: shell

    $ pip install pytest-spark


Usage
=====

Set Spark location
------------------

To run tests with required spark_home location you need to define it by
using one of the following methods:

1. Specify command line option "--spark_home"::

    $ pytest --spark_home=/opt/spark

2. Add "spark_home" value to ``pytest.ini`` in your project directory::

    [pytest]
    spark_home = /opt/spark

3. Set the "SPARK_HOME" environment variable.

pytest-spark will try to import ``pyspark`` from provided location.


.. note::
    "spark_home" will be read in the specified order. i.e. you can
    override ``pytest.ini`` value by command line option.


Customize spark_options
-----------------------

Just define "spark_options" in your ``pytest.ini``, e.g.::

    [pytest]
    spark_home = /opt/spark
    spark_options =
        spark.app.name: my-pytest-spark-tests
        spark.executor.instances: 1
        spark.jars.packages: com.databricks:spark-xml_2.12:0.5.0


Using the ``spark_context`` fixture
-----------------------------------

Use fixture ``spark_context`` in your tests as a regular pyspark fixture.
SparkContext instance will be created once and reused for the whole test
session.

Example::

    def test_my_case(spark_context):
        test_rdd = spark_context.parallelize([1, 2, 3, 4])
        # ...


Using the ``spark_session`` fixture (Spark 2.0 and above)
---------------------------------------------------------

Use fixture ``spark_session`` in your tests as a regular pyspark fixture.
A SparkSession instance with Hive support enabled will be created once and reused for the whole test
session.

Example::

    def test_spark_session_dataframe(spark_session):
        test_df = spark_session.createDataFrame([[1,3],[2,4]], "a: int, b: int")
        # ...

Overriding default parameters of the ``spark_session`` fixture
--------------------------------------------------------------
By default ``spark_session`` will be loaded with the following configurations :

Example::

    {
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

You can override some of these parameters in your ``pytest.ini``.
For example, removing Hive Support for the spark session :

Example::

    [pytest]
    spark_home = /opt/spark
    spark_options =
        spark.sql.catalogImplementation: in-memory

Development
===========

Tests
-----

Run tests locally::

    $ docker-compose up --build


.. _pytest: http://pytest.org/
.. _Apache Spark: https://spark.apache.org/
