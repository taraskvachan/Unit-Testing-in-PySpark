import findspark
findspark.init('/home/taras/spark-3.5.4-bin-hadoop3')

import unittest
from pyspark.sql import SparkSession
from DataProc import DataProc 

class TestDataProc(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
        cls.processor = DataProc(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    #Test the highest_close_price_per_year method with valid data
    def test_highest_close_price_per_year(self):
  
        data = [
            ('01-01-2021', 150),
            ('02-01-2021', 160),
            ('03-01-2021', 145),
            ('01-01-2022', 170),
            ('02-01-2022', 165),
            ('03-01-2022', 180),
        ]
        columns = ['Date', 'Close']
        df = self.spark.createDataFrame(data, columns)

        result_df = self.processor.highest_close_price_per_year(df)
        expected_data = [160, 180] 
        result_data = [row['Close'] for row in result_df.collect()]
        self.assertEqual(result_data, expected_data, "The highest close prices per year are incorrect")
    
    #Test the method with missing values in the Close column
    def test_highest_close_price_per_year_no_data(self):

        data = [
            ('01-01-2021', 150),
            ('06-01-2021', None),
            ('12-31-2021', 155),
            ('01-01-2022', None),
            ('06-01-2022', 180),
            ('12-31-2022', None),
        ]
        columns = ['Date', 'Close']
        df = self.spark.createDataFrame(data, columns)

        result_df = self.processor.highest_close_price_per_year(df)
        expected_data = [155, 180] 
        result_data = [row['Close'] for row in result_df.collect()]
        self.assertEqual(result_data, expected_data, "The highest close prices per year with missing values are incorrect")

if __name__ == '__main__':
    unittest.main()
