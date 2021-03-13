import pytest
from docutils.writers import null
from pyspark.sql.types import StructType, StructField, StringType, FloatType, Row
import logging
import os
import sys
from pyspark.sql import SparkSession
from Glue_Job_SAS_To_Parquet import add_audit_cols



IS_PY2 = sys.version_info < (3,)
if not IS_PY2:
    os.environ['PYSPARK_PYTHON'] = 'python3'

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


def test_my_app(spark_session):
   ...
spark = SparkSession\
    .builder\
    .config("spark.executor.instances", "2")\
    .config("spark.executor.cores", "2")\
    .config("spark.executor.memory", "2g")\
    .config("spark.driver.memory", "2g")\
    .getOrCreate()


# setup
@pytest.fixture
def df():
    schema = StructType(
          [
            StructField(name="TAX_CODE", dataType=StringType(), nullable=False),
            StructField(name="TAX_SLAB_INC88", dataType=FloatType(), nullable=False),
            StructField(name="TAX_SLAB_TAX88", dataType=FloatType(), nullable=False),
            StructField(name="TAXSLAB_INC89", dataType=FloatType(), nullable=False),
            StructField(name="TAXSLAB_TAX89", dataType=FloatType(), nullable=False),
          ]
        )
    rows = [
            # TAX_CODE       TAX_SLAB_INC88	        TAX_SLAB_TAX88	   TAXSLAB_INC89	 TAXSLAB_TAX89
            Row('BAKU',   9.21500015258789,     1.6430000066757202, 9.517999649047852,  2.125),
            Row('NGNK',   2.046999931335449,	0.4129999876022339,	2.068000078201294,	0.5649999976158142),
            Row('KKGX',   9.98900032043457,	    1.7519999742507935,	9.991999626159668,	2.2209999561309814),
            Row('MBUK',   8.321000099182129,	1.4079999923706055,	8.515000343322754,	1.9049999713897705),
            Row('NGUK',   4.5879998207092285,	0.8379999995231628,	4.388999938964844,	0.9430000185966492),
            Row('MVBG',   4.736000061035156,	0.7480000257492065,	5.014999866485596,	1.0509999990463257),
            Row('MKUV',   3.5959999561309814,	0.5770000219345093,	3.811000108718872,	0.8190000057220459),
            Row('WXYG',   4.829999923706055,	0.7519999742507935,	4.939000129699707,	1.0149999856948853),
            Row('VXGH',   4.507999897003174,	0.7609999775886536,	4.539000034332275,	1.0959999561309814),
    ]
    parellizeRows = spark.sparkContext.parallelize(rows)
    df = spark.createDataFrame(parellizeRows, schema)
    df.show()
    return df

# set expectations
@pytest.fixture
def df_with_audit_cols():
    schema = StructType(
        [
            StructField(name="TAX_CODE", dataType=StringType(), nullable=False),
            StructField(name="TAX_SLAB_INC88", dataType=FloatType(), nullable=False),
            StructField(name="TAX_SLAB_TAX88", dataType=FloatType(), nullable=False),
            StructField(name="TAXSLAB_INC89", dataType=FloatType(), nullable=False),
            StructField(name="TAXSLAB_TAX89", dataType=FloatType(), nullable=False),
            StructField(name="operation", dataType=StringType(), nullable=False),
            StructField(name="changedate", dataType=StringType(), nullable=False),
            StructField(name="changedate_year", dataType=StringType(), nullable=False),
            StructField(name="changedate_month", dataType=StringType(), nullable=False),
            StructField(name="changedate_day", dataType=StringType(), nullable=False),
        ]
    )
    rows = [
        # TAX_CODE    TAX_SLAB_INC88	    TAX_SLAB_TAX88	   TAXSLAB_INC89	    TAXSLAB_TAX89    operation    changedate    changedate_year   changedate_month changedate_day
        Row('BAKU', 9.21500015258789,   1.6430000066757202,     9.517999649047852,        2.125,          'I',    '2020-7-28',      '2020',               '7',             '28'),
        Row('NGNK',   2.046999931335449,	0.4129999876022339,	2.068000078201294,	0.5649999976158142,   'I',    '2020-7-28',      '2020',               '7',             '28'),
        Row('KKGX',   9.98900032043457,	    1.7519999742507935,	9.991999626159668,	2.2209999561309814,   'I',    '2020-7-28',      '2020',               '7',             '28'),
        Row('MBUK',   8.321000099182129,	1.4079999923706055,	8.515000343322754,	1.9049999713897705,   'I',    '2020-7-28',      '2020',               '7',             '28'),
        Row('NGUK',   4.5879998207092285,	0.8379999995231628,	4.388999938964844,	0.9430000185966492,   'I',    '2020-7-28',      '2020',               '7',             '28'),
        Row('MVBG',   4.736000061035156,	0.7480000257492065,	5.014999866485596,	1.0509999990463257,   'I',    '2020-7-28',      '2020',               '7',             '28'),
        Row('MKUV',   3.5959999561309814,	0.5770000219345093,	3.811000108718872,	0.8190000057220459,  'I',    '2020-7-28',      '2020',                '7',             '28'),
        Row('WXYG',   4.829999923706055,	0.7519999742507935,	4.939000129699707,	1.0149999856948853,  'I',    '2020-7-28',      '2020',                '7',             '28'),
        Row('VXGH',   4.507999897003174,	0.7609999775886536,	4.539000034332275,	1.0959999561309814,  'I',    '2020-7-28',      '2020',                '7',             '28'),
    ]
    parellizeRows = spark.sparkContext.parallelize(rows)
    df_with_audit_cols = spark.createDataFrame(parellizeRows, schema)
    df_with_audit_cols.show()
    return df_with_audit_cols

def test_add_audit_cols_with_fixtures(df, df_with_audit_cols):
    actual = add_audit_cols(df, '2020-7-28')
    expected = df_with_audit_cols
    actual.show()
    expected.show()
    assert actual, expected

def test_add_audit_cols_with_no_changedt(df, df_with_audit_cols):
    actual = add_audit_cols(df)
    expected = df_with_audit_cols
    actual.show()
    expected.show()
    assert (actual != expected), "Test is failed because of TypeError: add_audit_cols() missing 1 required positional argument: 'changedt'"

def test_add_audit_cols_with_invalid_changedt(df, df_with_audit_cols):
    actual = add_audit_cols(df, "TESTING1234")
    expected = df_with_audit_cols
    actual.show()
    expected.show()
    assert (actual, expected)

def test_add_audit_cols_with_null_changedt(df, df_with_audit_cols):
    actual = add_audit_cols(df, null)
    expected = df_with_audit_cols
    actual.show()
    expected.show()
    assert (actual, expected)


