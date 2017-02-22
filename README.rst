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

To run tests with required spark_home location just add "spark_home"
value to ``pytest.ini`` in your project directory::

    [pytest]
    spark_home = /opt/spark

pytest-spark will try to import ``pyspark`` from specified location.


Using fixture
-------------

Use fixture ``spark_context`` in your tests as a regular pyspark fixture.
SparkContext instance will be created once and reused for the whole test
session.

Example::

    def test_my_case(spark_context):
        test_rdd = spark_context.parallelize([1, 2, 3, 4])
        # ...



.. _pytest: http://pytest.org/
.. _Apache Spark: https://spark.apache.org/
