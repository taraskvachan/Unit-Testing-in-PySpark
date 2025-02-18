import findspark
findspark.init('/home/taras/spark-3.5.4-bin-hadoop3')

from pyspark.sql import Window, SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, substring


def create_spark_session():
    spark = SparkSession.builder \
        .appName('PySpark Class Example') \
        .master("local[*]") \
        .getOrCreate()
    return spark


class DataProc():

    @classmethod
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
    
    def read_data(self, path: str, format: str = 'csv')  -> DataFrame:
        return self.spark.read.csv(path, header=True, inferSchema=True)    

    def show_df(self, df: DataFrame):
        df.show()
    
    def highest_close_price_per_year(self, df):          
        window = Window.partitionBy(substring(col('Date'), 7, 4)).orderBy(col('Close').desc())
        return df.withColumn('rank', row_number().over(window)).filter(col('rank') == 1).drop('rank')  


if __name__ == '__main__':
    pass