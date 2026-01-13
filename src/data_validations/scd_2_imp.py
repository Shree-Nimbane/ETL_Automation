from functools import reduce
from pyspark.sql import functions as F
import os
from delta.tables import *
# @pytest.mark.usefixtures('red_config', 'spark_session')
class SCD_IMP:

    def __init__(self,source):
        self.source_df=source


    def scd_2(self,spark,primary_keys,compare_columns,TableName):
        source_df=self.source_df
        all_source_col = source_df.columns
        # C:\Users\shiva\PySpark_code\pycharmProject\oops_with_etl\spark-warehouse
        # target_path=(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        # target_path = f"./oops_with_etl/spark-warehouse/{TableName}"
        copy_df = source_df.select([F.col(c).alias("target_" + c) for c in source_df.columns])
        DeltaTable.createIfNotExists(spark) \
            .tableName(TableName) \
            .addColumns(copy_df.schema) \
            .addColumn("active_status", "STRING") \
            .addColumn("start_date", "TIMESTAMP") \
            .addColumn("end_date", "TIMESTAMP") \
            .execute()
        table_inst = DeltaTable.forName(spark, TableName)
        target_df = table_inst.toDF().filter("active_status = 'Y'")
        join_conditions = [F.col(f"source.{k}") == F.col(f"target.target_{k}")
            for k in primary_keys]
        join_condition = reduce(lambda a, b: a & b, join_conditions)
        source_cols = [F.col(f"source.{c}") for c in compare_columns]
        target_cols = [F.col(f"target.target_{c}") for c in compare_columns]
        changed_df = (
            source_df.alias("source")
            .join(target_df.alias("target"), join_condition, "left")
            .filter(F.xxhash64(*source_cols) != F.xxhash64(*target_cols)))

        changed_df = changed_df.withColumn("MergeKey",
            F.concat_ws("||", *primary_keys))
        condition = reduce(lambda a, b: a & b, [F.col(f"target_{c}").isNotNull() for c in primary_keys])
        dummy_df = changed_df.filter(condition).withColumn("MergeKey", F.lit(None))
        scd_df = changed_df.union(dummy_df)
        merge_condi = ((F.concat_ws("||", *[F.col(f"target.target_{i}") for i in primary_keys])
                              == F.col("source.MergeKey"))
                       & (F.col("target.active_status") == F.lit("Y")))

        insert_map = {f"target_{c}": f"source.{c}" for c in all_source_col}
        insert_map.update({"active_status": "'Y'",
            "start_date": "current_timestamp()",
            "end_date": "TIMESTAMP('9999-12-31')"})

        table_inst.alias("target") \
            .merge(scd_df.alias("source"), merge_condi) \
            .whenMatchedUpdate(set={
            "active_status": F.lit("N"),
            "end_date": F.current_timestamp()
        }) \
            .whenNotMatchedInsert(values=insert_map) \
            .execute()
        return table_inst.toDF()

