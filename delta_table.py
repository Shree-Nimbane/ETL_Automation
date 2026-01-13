
import pyspark
from delta import *
from delta.tables import *

# builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#
# # This helper function automatically adds the required Delta Lake JARs
# spark = configure_spark_with_delta_pip(builder).getOrCreate()


# df = spark.createDataFrame(
#     [(1, "A"), (2, "B")],
#     ["id", "name"]
# )
# # df.show()
# df.write.format("delta").mode("overwrite").save("data/delta/test")
#
# spark.read.format("delta").load("data/delta/test").show()

# DeltaTable.createIfNotExists(spark)\
# DeltaTable.create(spark)\
#     .tableName('temp')\
#     .addColumn('id','INT')\
#     .addColumn('fname','STRING')\
#     .addColumn('salary','INT')\
#     .addColumn('depart','STRING')\
#     .addColumn('gender','STRING')\
#     .location(r'C:\Users\Dinesh\PycharmProjects\Shree_wafa\New_delta')\
#     .execute()

# spark.sql('select * from temp')
# spark.sql('select * from temp')




from pyspark.sql import SparkSession

snow_jar = 'C:\\Users\\Dinesh\\PycharmProjects002\\shrinevas\\taf_dec\\jars\\snowflake-jdbc-3.22.0.jar'
postgres_jar = 'C:\\Users\\Dinesh\\PycharmProjects002\\mahender_automation\\jars\\postgresql-42.7.3.jar'
jar_path = snow_jar+','+postgres_jar


spark = (SparkSession.builder.master("local[4]")
        .appName("pytest_framework")
        .config("spark.jars", jar_path)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        )
spark=configure_spark_with_delta_pip(spark).getOrCreate()

DeltaTable.createIfNotExists(spark)\
    .tableName('temp')\
    .addColumn('id','INT')\
    .addColumn('fname','STRING')\
    .addColumn('salary','INT')\
    .addColumn('depart','STRING')\
    .addColumn('gender','STRING')\
    .location(r'C:\Users\Dinesh\PycharmProjects\Shree_wafa\New_delta')\
    .execute()
#
spark.sql('insert into temp values (101,"shree",10000000,"IT","M")')
# spark.sql('delete from temp')
# spark.sql('insert into temp values (100,"shree",10000000,"IT","M")')
# spark.sql('select * from temp').show()

df=spark.read.csv(r'C:\Users\Dinesh\PycharmProjects\oops_with_etl\input_files\Reading_all_data.csv',header=True)

df.write.format('delta').mode('overwrite').saveAsTable('employee20')
# df.write.insertInto('employee20',overwrite=True)
# df.write.format('delta').mode('append').saveAsTable('employee19')
# spark.sql('delete table employee21')
# spark.sql('select * from employee20').show()

# deltaInstance=DeltaTable.forPath(spark,'C:\\Users\\Dinesh\\PycharmProjects\\Shree_wafa\\New_delta')
# deltaInstance=DeltaTable.forName(spark,'temp')
deltaInstance=DeltaTable.forName(spark,'employee20')
deltaInstance.toDF().show()
print(type(deltaInstance))

# deltaInstance.delete('id=100')
# spark.sql('select * from temp')
# deltaInstance.toDF().show()