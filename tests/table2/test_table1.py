from src.utility.Base_Class import BaseClass

from src.data_validations import data_quantity

class Test_Fitst_class(BaseClass):

    def test_spark(self):
        return True


    def test_count_check(self):

        data_quality_check = data_quantity.Data_Quantity(*self.read_data)
        status=data_quality_check.conunt_val()
        print(status)
        assert status=='PASS'




