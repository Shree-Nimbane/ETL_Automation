from src.data_validations import data_quantity
from src.utility.Base_Class import BaseClass
from src.data_validations import scd_2_imp

# target_path="C:/Users/Dinesh/spark-warehouse/pre_delta"
primary_keys=['ID','Depart']
value_columns=['Fname','Lname','Salary','City','Blould']
class Test_Fitst_class(BaseClass):


    def test_count_check(self):
        data_quality_check=data_quantity.Data_Quantity(*self.read_data)
        status=data_quality_check.conunt_val()
        assert status == 'PASS'


    def test_only_in_source(self):
        key_columns=self.read_yml['validations']['count_check']['key_columns']
        only_source = data_quantity.Data_Quantity(*self.read_data)
        only_source.recodes_only_in_source(key_columns=key_columns)


    def test_scd2_check(self):
        primary_keys = ["ID", "Depart"]  # change as per your table
        compare_columns = ["Fname", "Lname", "Salary", "City", "Blould"]
        spark = self.spark
        source,target=self.read_data
        scd_obj=scd_2_imp.SCD_IMP(source)
        quality_check=scd_obj.scd_2(spark=spark,TableName='krishna',primary_keys=primary_keys,compare_columns=compare_columns)
        quality_check.show()


    # def test_scd_under_imp(self):
    #     source_df,target_df=self.read_data
    #     print()
    #     self.scd_2(source_df=source_df).show()


    # def test_under_scd(self):
    #     source_df, target_df = self.read_data
    #     value=self.scd_2_dynamic(source_df,target_path,primary_keys,value_columns)



