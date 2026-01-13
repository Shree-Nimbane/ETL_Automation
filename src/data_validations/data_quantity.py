

class Data_Quantity:
    'testing some data quality'

    def __init__(self,source,target):
        self.source_df=source
        self.target_df=target


    def conunt_val(self):

        if self.source_df.count()==self.target_df.count():
            status= 'PASS'
        else:
            status= 'FAIL'
        return status

    def recodes_only_in_source(self,key_columns):
        only_in_source=self.source_df.select(key_columns).exceptAll(self.target_df.select(key_columns))

        count_in_source=only_in_source.count()
        if count_in_source >0:
            Status='FAIL'
            failed_recodes=only_in_source.limit(4).collect()
            failed_review=[row.asDict() for row in failed_recodes]
        else:
            Status='PASS'
        return Status


    def recodes_only_in_target(self,key_columns):
        only_in_target = self.target_df.select(key_columns).exceptAll(self.source_df.select(key_columns))

        count_in_target = only_in_target.count()
        if count_in_target > 0:
            Status = 'FAIL'
            failed_recodes = only_in_target.limit(4).collect()
            failed_review = [row.asDict() for row in failed_recodes]
        else:
            Status = 'PASS'
        return Status