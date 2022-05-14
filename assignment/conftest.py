from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope='session')
def spark_s():
    """Method that launch spark session for testing purpose, closes spark session 
    when testing session ends
    """
    builder = (
        SparkSession
        .builder
        .master("local")
        .appName("testing_session")
        )
    spark_s = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark_s
    spark_s.stop()