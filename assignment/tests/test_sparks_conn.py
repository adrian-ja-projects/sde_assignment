#Author: Adrian J 2022-05
from delta import * 
from pyspark.sql import functions as F
from pyspark.sql import SparkSession 
import pytest

def spark_conn(spark: SparkSession):
    simpleData = [("Hello","World!")]
    columns= ["first_word","second_word"]
    df = spark.createDataFrame(data = simpleData, schema = columns)
    row_count = df.count()
    word = f"{df.select("first_word").collect()[0][0]} {df.select("second_word").collect()[0][0]}"
    return row_count, word

def test_spark_conn(spark_s):
    assert spark_conn(spark_s)[0] == 1
    assert spark_conn(spark_s)[0] == "Hello World!"