from delta import *
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

snow_jar = 'C:\\Users\\Dinesh\\PycharmProjects002\\shrinevas\\taf_dec\\jars\\snowflake-jdbc-3.22.0.jar'
postgres_jar = 'C:\\Users\\Dinesh\\PycharmProjects002\\mahender_automation\\jars\\postgresql-42.7.3.jar'
jar_path = snow_jar+','+postgres_jar


spark = (SparkSession.builder.master("local[1]")
        .appName("pytest_framework")
        .config("spark.jars", jar_path)
        .config("spark.sql.warehouse.dir","C:/Users/Dinesh/spark-warehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        )
spark=configure_spark_with_delta_pip(spark).getOrCreate()

# spark.sql('''CREATE TABLE audit_log (
#   operation STRING ,
#   updated_time timestamp ,
#   user_name STRING,
#   notebook_name STRING,
#   numTargetRowsUpdated int,
#   numTargetRowsInsert int,
#   numTargetRowsDeleted int
#   )
# ''')
# spark.sql('select * from audit_log')

spark.sql("""
CREATE TABLE audit1_log (
  operation STRING,
  numTargetRowsDeleted INT,
  numTargetRowsInserted INT,
  numTargetRowsUpdated INT
)
USING PARQUET
LOCATION 'C:/Users/Dinesh/spark-warehouse/audit1_log'
""")
# spark.sql(' delete table audit1_log')
instance=DeltaTable.forPath(spark,"C:/Users/Dinesh/spark-warehouse/dim_employee")
instance.toDF().show()
value_to_explode=instance.history(1)

value=value_to_explode.select(value_to_explode.operation,explode(value_to_explode.operationMetrics))


after_cast=value.select(value.operation,value.key,value.value.cast('int'))

# after_cast.show(truncate=False)
# how to pivot

after_pivot=after_cast.groupBy('operation').pivot('key').sum('value')
# spark.sql('delete from audit1_log')
all_clean=after_pivot.select(after_pivot.operation,after_pivot.numTargetRowsDeleted,after_pivot.numTargetRowsInserted,after_pivot.numTargetRowsMatchedUpdated)
all_clean.createOrReplaceTempView("audit")

spark.sql('select * from audit').show(truncate=False)
# spark.sql('insert into audit1_log select * from audit')

# spark.sql('select * from audit1_log').show(truncate=False)


# spark.sql('''CREATE OR REPLACE TABLE dim_employee (
#   emp_id INT ,
#   name STRING ,
#   city STRING,
#   country STRING,
#   contact INT
#   )
# USING DELTA
# LOCATION "C:/Users/Dinesh/spark-warehouse/dim_employee"
# ''')
