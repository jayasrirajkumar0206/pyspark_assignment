import pytest
from pyspark.sql import SparkSession
from src.Question4.util import *


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Test Question4") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_camel_to_snake():
    assert camel_to_snake("employeeName") == "employee_name"


def test_partition_columns(spark):

    df = spark.createDataFrame(
        [("0001",)],
        ["id"]
    )

    df = add_load_date(df)
    df = add_partition_columns(df)

    assert "year" in df.columns
    assert "month" in df.columns
    assert "day" in df.columns