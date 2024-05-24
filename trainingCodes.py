from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName("TEK Training").getOrCreate()
    data = [("abc"), ("def"), ("ghi"), ("jkl"), ("mno")]

    df = spark.createDataFrame(data, schema = "name")

    df.show()