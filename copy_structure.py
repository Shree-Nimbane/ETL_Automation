from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from delta.tables import *

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

df=spark.read.csv(r'C:\Users\Dinesh\PycharmProjects\oops_with_etl\input_files\Reading_all_data.csv',header=True)
# original_df.show()
df = df.select("*").where("1=2")
target_df=df.withColumn('active_status',lit('Y')).withColumn('start_date',F.current_timestamp()).\
        withColumn('end_date',F.lit("9999-12-31").cast("date"))
target_df.show(truncate=False)

target_df.write.format('delta').mode('overwrite').saveAsTable('try_first')

# spark.sql('select * from try_first').show()


















# # Assuming 'original_df' is your existing PySpark DataFrame
# copied_structure_df = original_df.select("*").where("1=2")
# #
# # # Alternatively, using a condition that is always false to return no rows
# empty_df = original_df.filter("false")
# #
# # # View the schema of the new empty DataFrame
# empty_df.printSchema()
