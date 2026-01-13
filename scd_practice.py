from delta import *
from delta.tables import *
from pyspark.sql import SparkSession

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


from pyspark.sql.types import *
from pyspark.sql.functions import *
schema=StructType([StructField('id', IntegerType()), StructField('name', StringType()),StructField('city', StringType()),StructField('country', StringType()),StructField('contact', IntegerType())])
data=[(1001,"Michael",'New_York','USA',123456789)]
df=spark.createDataFrame(data,schema)
# df.show()

# df.createOrReplaceTempView('source_view')
# spark.sql('select * from source_view').show()

spark.sql('''CREATE OR REPLACE TABLE dim_employee (
  emp_id INT ,
  name STRING ,
  city STRING,
  country STRING,
  contact INT
  )
USING DELTA
LOCATION "C:/Users/Dinesh/spark-warehouse/dim_employee"
''')
#
# spark.sql('''MERGE INTO dim_employee as target
# USING source_view as source
# on target.emp_id =source.id
# when MATCHED
# THEN UPDATE SET
# target.name=source.name,
# target.city=source.city,
# target.country=source.country,
# target.contact=source.contact
# WHEN NOT MATCHED THEN
# INSERT (emp_id,name,country,city,contact) values(id,name,country, city,contact)''')
#
# spark.sql("select * from dim_employee").show()
dim_emp=DeltaTable.forName(spark,'dim_employee')

# dim_emp.toDF().show()
#       using pyspark method
df.write.format('delta').mode('overwrite').saveAsTable('source_delta1')
source_instance=DeltaTable.forPath(spark,"C:/Users/Dinesh/spark-warehouse/source_delta1")
# source_instance=DeltaTable.forName(spark,'source_delta1')

# source_instance.toDF().show()


dim_emp.alias('target').merge(
        source=df.alias('source'),condition="target.emp_id=source.id"
).whenMatchedUpdate(set={
        'name':'source.name',
        'city':'source.city',
        'country':'source.country',
        'contact':'source.contact'
}).whenNotMatchedInsert(values={
        'emp_id':'source.id',
        'name':'source.name',
        'city':'source.city',
        'country':'source.country',
        'contact':'source.contact'
}).execute()

dim_emp.toDF().show()