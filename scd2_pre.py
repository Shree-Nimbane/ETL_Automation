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

# spark.sql("DROP TABLE IF EXISTS scd2demo ")
# spark.sql("DROP TABLE IF EXISTS scd2demo PURGE")


spark.sql('''CREATE OR REPLACE TABLE scd2_Demo (
  emp_id INT ,
  name STRING ,
  city STRING,
  country STRING,
  contact INT,
  active_status STRING,
  sart_date TIMESTAMP,
  end_date TIMESTAMP
  )
USING DELTA
LOCATION "C:/Users/Dinesh/spark-warehouse/scd2_Demo"
''')

# spark.sql('delete table scd2_Demo')
# spark.sql("""
# INSERT INTO scd2_Demo VALUES
# (5, 'Ravi', 'Pune', 'India', 987654321, 'Y', current_timestamp(), NULL),
# (2, 'Amit', 'Mumbai', 'India', 912345678, 'Y', current_timestamp(), NULL),
# (3, 'Neha', 'Delhi', 'India', 998877665, 'Y', current_timestamp(), NULL)
# """)
spark.sql("""
INSERT INTO scd2_Demo VALUES
(5, 'Ravi', 'hvkj', 'Infgdia', 987654321, 'Y', current_timestamp(), NULL),
(2, 'Amit', 'Mdsfumbai', 'Indsfdia', 912345678, 'Y', current_timestamp(), NULL),
(3, 'Neha', 'Delhdi', 'Isdfndia', 998877665, 'Y', current_timestamp(), NULL)
""")

# spark.sql('select * from scd2_Demo').show()
# spark.sql('delete table scd2_Demo')
target_table=DeltaTable.forPath(spark,"C:/Users/Dinesh/spark-warehouse/scd2_Demo")
target_df=target_table.toDF()

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, TimestampType
)

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("contact", IntegerType(), True)
])
from pyspark.sql import Row
from datetime import datetime

data = [(1, "Ravi", "Pune", "India", 987654321),
    (2, "Amit", "Delhi", "India", 912345678),
    (4, "Raja", "Mumbai", "India", 998877451)]
source_df = spark.createDataFrame(data, schema)

join_df=source_df.join(target_df,(source_df.emp_id==target_df.emp_id)& \
                       (source_df.name==target_df.name) &\
                       (target_df.active_status=="Y"),"leftouter")\
    .select(source_df["*"],\
            target_df.emp_id.alias('target_id'),\
            target_df.name.alias('target_name'),\
            target_df.city.alias('target_city'),\
            target_df.country.alias('target_country'),\
            target_df.contact.alias('target_contact'))


from pyspark.sql.functions import xxhash64

afterDf=join_df.filter(xxhash64(join_df.city,join_df.country,join_df.contact)
               !=xxhash64(join_df.target_city,join_df.target_country,join_df.target_contact))

# we removed match record means we have changed value and new added value

from pyspark.sql.functions import concat,lit
margeDF=afterDf.withColumn("MergeKey",concat(afterDf.emp_id,afterDf.name))
margeDF.show(truncate=False)
#--------------------------------
# remove newly added value
dummy_df=afterDf.filter("target_id is not null").withColumn('MergeKey',lit(None))
dummy_df.show(truncate=False)

scd_DF=margeDF.union(dummy_df)
# scd_DF = scd_DF.alias("source")
scd_DF.show(truncate=False)
target_df.show(truncate=False)


# 1. Filter out the null MergeKeys to prevent join failures
final_scd_df = scd_DF.filter("MergeKey IS NOT NULL")

# 2. Execute Merge with simplified Date casting
target_table.alias('target').merge(
    source=final_scd_df.alias('source'),
    condition="source.MergeKey = concat(target.emp_id, target.name) AND target.active_status = 'Y'"
).whenMatchedUpdate(set={
    'active_status': "'N'",
    'end_date': 'current_timestamp()',
}).whenNotMatchedInsert(values={
    'emp_id': 'source.emp_id',
    'name': 'source.name',
    'city': 'source.city',
    'country': 'source.country',
    'contact': 'source.contact',
    'active_status': "'Y'",
    'sart_date': 'current_timestamp()',
    'end_date': "CAST('9999-12-31' AS TIMESTAMP date)"
}).execute()

# 3. Show results
spark.sql("SELECT * FROM scd2_Demo ORDER BY emp_id, active_status DESC").show()

# target_df.drop()




