from pyspark.sql.functions import col, trim
# from src.utility.report_lib import write_output


class DataQuality:
    ''' Duplicate data/ Rejected data/
     data validation rules / data integrity primary keys / (derived column)calculations\
     date column should be in year/ '''

    def __init__(self,source_df,target_df):
        self.source_df=source_df
        self.target_df=target_df


    def null_validation(self,df,null_columns):
        for column in null_columns:
            failing_roc=df.filter(col(column).isNull) |(trim(col(column))=='')
            null_count=failing_roc.count()
            if null_count>0:
                rows=failing_roc.limit(5).collect()
                failed_preview=[row.asDict() for row in rows]




