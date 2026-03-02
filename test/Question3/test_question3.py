import pytest
from pyspark.sql import SparkSession
from src.Question3.util import *


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Test Question3") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_rename_columns(spark):

    df = create_log_df(spark)

    df = rename_columns_dynamic(df)

    assert "log_id" in df.columns
    assert "user_id" in df.columns


def test_login_date(spark):

    df = create_log_df(spark)
    df = rename_columns_dynamic(df)

    df = convert_login_date(df)

    assert "login_date" in df.columns


def test_last_7_days(spark):

    df = create_log_df(spark)
    df = rename_columns_dynamic(df)

    result = last_7_days_activity(df)

    assert "action_count" in result.columns