import pytest
from pyspark.sql import SparkSession
from src.Question5.util import *


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Test Question5") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_avg_salary(spark):

    emp_df = create_employee_df(spark)

    result = avg_salary_department(emp_df)

    assert "avg_salary" in result.columns


def test_bonus(spark):

    emp_df = create_employee_df(spark)

    df = add_bonus(emp_df)

    assert "bonus" in df.columns


def test_country_replace(spark):

    emp_df = create_employee_df(spark)
    country_df = create_country_df(spark)

    df = country_replace(emp_df, country_df)

    assert "state" in df.columns