import pytest
from pyspark.sql import SparkSession
from src.Question1.util import *


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Test Question1") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_only_iphone13(spark):

    purchase_df = create_purchase_df(spark)

    result = customers_only_iphone13(purchase_df).collect()

    customers = [row.customer for row in result]

    assert 4 in customers


def test_upgraded_customers(spark):

    purchase_df = create_purchase_df(spark)

    result = upgraded_customers(purchase_df).collect()

    customers = [row.customer for row in result]

    assert 1 in customers
    assert 3 in customers


def test_all_products(spark):

    purchase_df = create_purchase_df(spark)
    product_df = create_product_df(spark)

    result = customers_all_products(purchase_df, product_df).collect()

    customers = [row.customer for row in result]

    assert 1 in customers