import pytest
from pyspark.python.pyspark.shell import spark
from pyspark.sql.types import StructType, StructField, FloatType, StringType, Row
import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import logging
from datetime import datetime, timedelta
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import Job
import boto3

from pytest_spark.config import SparkConfigBuilder
from pytest_spark.util import reduce_logging
import py4j


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
        session = SparkSession.builder \
            .config(conf=SparkConfigBuilder().get()) \
            .getOrCreate()

        yield session
        session.stop()


@pytest.fixture(scope='session')
def spark_context(_spark_session):
    """Return a SparkContext instance with reduced logging
    (session scope).
    """

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

def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)

@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark = (SparkSession
             .builder
             .master('local[2]')
             .config('spark.jars.packages', 'com.databricks:spark-avro_2.11:3.0.1')
             .appName('pytest-pyspark-local-testing')
             .enableHiveSupport()
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark


# @pytest.fixture(scope='session')
# def spark_session(_spark_session):
#     """Return a Hive enabled SparkSession instance with reduced logging
#     (session scope).
#
#     Available from Spark 2.0 onwards.
#     """
#
#     if _spark_session is None:
#         raise Exception(
#             'The "spark_session" fixture is only available on spark 2.0 '
#             'and above. Please use the spark_context fixture and instanciate '
#             'a SQLContext or HiveContext from it in your tests.'
#         )
#     else:
#         reduce_logging(_spark_session.sparkContext)
#         yield _spark_session



