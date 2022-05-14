from pyspark.sql import functions as F

def test_spark_running(spark_s):
    data = [("Hello", "World", "!")]
    columns = ["First_Word", "Second_Word", "Exclamation"]
    df = spark_s.createDataFrame(data, columns)
    hello = df.select("First_Word").collect()[0][0]
    world = df.select("Second_Word").collect()[0][0]
    exclamation = df.select("Exclamation").collect()[0][0]
    hello_world = f"{hello} {world} {exclamation}"
    assert hello_world == "Hello World !"