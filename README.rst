pytest-spark
############

pytest_ plugin to run the tests with support of pyspark (`Apache Spark`_).

This plugin will allow to specify SPARK_HOME directory in ``pytest.ini``
and thus to make "pyspark" importable in your tests which are executed
by pytest.

Also it defines session scope fixture ``spark_context`` which can be
used in your tests.


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

.. _pytest: http://pytest.org/
.. _Apache Spark: https://spark.apache.org/
