
Using the ``spark_context`` fixture
-----------------------------------

Use fixture ``spark_context`` in your tests as a regular pyspark fixture.
SparkContext instance will be created once and reused for the whole test
session.

Example::

    def test_my_case(spark_context):
        test_rdd = spark_context.parallelize([1, 2, 3, 4])
        # ...


Using the ``spark_session`` fixture
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
