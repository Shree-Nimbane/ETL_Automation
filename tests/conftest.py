import yaml
import os
from delta import *
from pyspark.sql import SparkSession
import pytest

# snowflake='C:/Users/shiva/PySpark_code/pycharmProject/oops_with_etl/jars/snowflake-jdbc-3.22.0.jar'
taf_path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
jar_path = os.path.join(taf_path,"jars","snowflake-jdbc-3.22.0.jar")
warehouse_location = os.path.abspath("spark-warehouse")
@pytest.fixture(scope='class')
def spark_session(request):
    spark = (SparkSession.builder.master("local[1]")
             .appName("pytest_framework")
             .config("spark.jars", jar_path)
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") #
             .config("spark.sql.warehouse.dir", warehouse_location) 
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.driver.extraClassPath", jar_path)
             .config("spark.executor.extraClassPath", jar_path)
             )
    spark = configure_spark_with_delta_pip(spark).getOrCreate()
    request.cls.spark = spark

@pytest.fixture(scope="class")
def red_config(request):
    # global value
    path=request.node.fspath.dirname
    request.cls.path=path



# snowflake='C:\\Users\\Dinesh\\PycharmProjects002\\shrinevas\\taf_dec\\jars\\snowflake-jdbc-3.22.0.jar'
# jar_path = snowflake
# @pytest.fixture(scope='class')
# def spark_session(request):
#         spark = SparkSession.builder \
#         .master("local[1]") \
#         .appName("Automation.com") \
#         .config("spark.jars",jar_path)\
#         .config('spark.driver.extraClass.Path',jar_path)\
#         .config('spark.executor.extraClass.Path',jar_path)\
#         .getOrCreate()
#         request.cls.spark=spark





