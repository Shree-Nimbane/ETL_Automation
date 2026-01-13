from delta import *
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

snow_jar = 'C:\\Users\\shiva\\PySpark_code\\taf_dec\\jars\\snowflake-jdbc-3.22.0.jar'
postgres_jar = 'C:\\Users\\shiva\\PySpark_code\\taf_dec\\jars\\postgresql-42.7.5.jar'
jar_path = snow_jar+','+postgres_jar


spark = (SparkSession.builder.master("local[1]")
        .appName("pytest_framework")
        .config("spark.jars", jar_path)
        .config("spark.sql.warehouse.dir","C:/Users/shiva/spark-warehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        )
spark=configure_spark_with_delta_pip(spark).getOrCreate()

# spark.sql("DROP TABLE IF EXISTS scd2demo ")
# spark.sql("DROP TABLE IF EXISTS scd2demo PURGE")


spark.sql('''CREATE TABLE IF NOT EXISTS fact_table (
  pk1 INT ,
  pk2 INT ,
  dim1 INT,
  dim2 INT,
  dim3 INT,
  dim4 INT,
  active_status STRING,
  start_date TIMESTAMP,
  end_date TIMESTAMP
  )
USING DELTA
LOCATION "C:/Users/shiva/spark-warehouse"
''')
# spark.sql("""
# INSERT INTO fact_table VALUES
# (100, '100', '10', '20', '40','59', 'Y', current_timestamp(), '9999-12-31'),
# (101, '101', '300', '400', '500','600', 'Y', current_timestamp(), '9999-12-31'),
# (102, '102', '3000', '4000', '5000', '6000','Y', current_timestamp(), '9999-12-31')
# """)
# spark.sql("""
# INSERT INTO fact_table VALUES
# (103, '103', '3000', '4000', '5000','6000', 'Y', current_timestamp(), '9999-12-31'),
# (104, '104', '55', '66', '77','88', 'Y', current_timestamp(), '9999-12-31'),
# (102, '102', '333', '444', '555', '666','Y', current_timestamp(), '9999-12-31')
# """)
print('fact table created ')
spark.sql('select * from fact_table').show()

# target_table=DeltaTable.forPath(spark,"C:/Users/Dinesh/spark-warehouse/fact_table")
target_table=DeltaTable.forName(spark,'fact_table')
target_df=target_table.toDF()

from pyspark.sql.types import IntegerType

schema = StructType([
        StructField("pk1", IntegerType(), True),
        StructField("pk2", IntegerType(), True),
        StructField("dim1", IntegerType(), True),
        StructField("dim2", IntegerType(), True),
        StructField("dim3", IntegerType(), True),
        StructField("dim4", IntegerType(), True)])

data = [(103, 103, 400, 500, 600,60),
        (104, 104, 900, 1000, 2000,3000),
    (102, 102, 30, 40, 5,60)]
source_df = spark.createDataFrame(data, schema)
print('source data frame created')
source_df.show()
join_df=source_df.join(target_df,(source_df.pk1==target_df.pk1)& \
                       (source_df.pk2==target_df.pk2) & \
                       (target_df.active_status=="Y"),"leftouter")\
    .select(source_df["*"],\
            target_df.pk1.alias('target_pk1'),\
            target_df.pk2.alias('target_pk2'),\
            target_df.dim1.alias('target_dim1'),\
            target_df.dim2.alias('target_dim2'),\
            target_df.dim3.alias('target_dim3'),\
            target_df.dim4.alias('target_dim4'))
print("joined opration")
join_df.show(truncate=False)
from pyspark.sql.functions import xxhash64

filterDf=join_df.filter(xxhash64(join_df.dim1,join_df.dim2,join_df.dim3,join_df.dim4)
               != xxhash64(join_df.target_dim1,join_df.target_dim2,join_df.target_dim3,join_df.target_dim4))
print('we removed mach record after filter')
filterDf.show()
# # we removed match record means we have changed value and new added value
#
from pyspark.sql.functions import concat,lit
margeDF=filterDf.withColumn("MergeKey",concat(filterDf.pk1,filterDf.pk2))
print('we created merge key')
margeDF.show(truncate=False)
# # # #--------------------------------
# # # remove newly added value
dummy_df=margeDF.filter("target_pk1 is not null").withColumn('MergeKey',lit(None))
print('we find changed value')
dummy_df.show(truncate=False)
#
scd_DF=margeDF.union(dummy_df)
print('we joined dublicate reored for scd 2')
# # # scd_DF = scd_DF.alias("source")
scd_DF.show(truncate=False)
# # # target_df.show(truncate=False)
# # # final_scd_DF = scd_DF.filter("MergeKey IS NOT NULL")
print('before merge')
target_df.show()
target_table.alias('target').merge(
        source=scd_DF.alias('source'),
        condition="concat(target.pk1,target.pk2)=source.MergeKey and target.active_status='Y'"
).whenMatchedUpdate(set={
        "active_status":'"N"',
        "end_date":"current_timestamp()"
}).whenNotMatchedInsert(
        values={
                'pk1': 'source.pk1',
                'pk2' :'source.pk2' ,
                'dim1':"source.dim1",
                "dim2":"source.dim2",
                "dim3" :'source.dim3',
                'dim4' :'source.dim4',
                'active_status' :"'Y'",
                "start_date" :"current_timestamp()",
                'end_date': """CAST('9999-12-31' AS TIMESTAMP)""" }).execute()
target_df.show()
from pyspark.sql import SparkSession

# Assumes you have a spark session configured with delta lake support
# spark = SparkSession.builder.appName("DeltaTableClear").getOrCreate()

# table_name = "fact_table" # Use the registered table name
# spark.sql(f"TRUNCATE TABLE {table_name}")


# target_df.show()
# # 1. Filter out the null MergeKeys to prevent join failures
# final_scd_df = scd_DF.filter("MergeKey IS NOT NULL")
#
# # 2. Execute Merge with simplified Date casting
# target_table.alias('target').merge(
#     source=final_scd_df.alias('source'),
#     condition="source.MergeKey = concat(target.emp_id, target.name) AND target.active_status = 'Y'"
# ).whenMatchedUpdate(set={
#     'active_status': "'N'",
#     'end_date': 'current_timestamp()',
# }).whenNotMatchedInsert(values={
#     'emp_id': 'source.emp_id',
#     'name': 'source.name',
#     'city': 'source.city',
#     'country': 'source.country',
#     'contact': 'source.contact',
#     'active_status': "'Y'",
#     'sart_date': 'current_timestamp()',
#     'end_date': "CAST('9999-12-31' AS TIMESTAMP)"
# }).execute()
#
# # 3. Show results
# spark.sql("SELECT * FROM scd2_Demo ORDER BY emp_id, active_status DESC").show()
#
# # target_df.drop()
#
#
#
#
