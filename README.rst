pytest-spark
############

pytest_ plugin to run the tests with support of pyspark (`Apache Spark`_).

This plugin will allow to specify SPARK_HOME directory in ``pytest.ini``
and make "pyspark" importable in you tests which are executed by pytest.


Install
=======

.. code-block:: shell

    $ pip install pytest-spark

Usage
=====

To run test with required spark_home location just add "spark_home"
value to ``pytest.ini`` in your project directory::

    [pytest]
    spark_home = /opt/spark

pytest-spark will try to import pyspark from specified location.



.. _pytest: http://pytest.org/
.. _Apache Spark: https://spark.apache.org/
