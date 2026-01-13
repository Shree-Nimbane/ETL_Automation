# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
# spark = SparkSession.builder \
#       .master("local[1]") \
#       .appName("SparkByExamples.com") \
#       .getOrCreate()
# spark = SparkSession.builder.appName("test").getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")
#
# dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
# df=spark.createDataFrame(dataList, schema=['Language','fee'])
#
# df.show()


import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col

snow_jar = 'C:\\Users\\Dinesh\\PycharmProjects002\\shrinevas\\taf_dec\\jars\\snowflake-jdbc-3.22.0.jar'
postgres_jar = 'C:\\Users\\Dinesh\\PycharmProjects002\\mahender_automation\\jars\\postgresql-42.7.3.jar'
jar_path = snow_jar+','+postgres_jar
spark = (SparkSession.builder.master("local[4]")
        .appName("pytest_framework")
        .config("spark.jars", jar_path)
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        .getOrCreate())


# df=spark.read.format("jdbc")\
# .option('url',"jdbc:snowflake://gryzysc-de38647.snowflakecomputing.com/")\
# .option('driver', "net.snowflake.client.jdbc.SnowflakeDriver")\
# .option('user', 'MC')\
# .option('password', 'WZ98vXyNeK,%p4D')\
# .option('warehouse', 'COMPUTE_WH')\
# .option('db', 'MAHENDER')\
# .option('schema', 'TEST')\
# .option('dbtable', 'test_table_silver')\
# .load()

jdbc_url = "jdbc:oracle:thin:@localhost:1521:ORCL"

properties = {
    "user": "system",
    "password": "oracle",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="EMP",
    properties=properties
)


df.show()