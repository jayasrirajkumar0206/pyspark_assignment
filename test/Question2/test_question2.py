import pytest
from pyspark.sql import SparkSession
from src.Question2.util import *


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Test Question2") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()



def test_partition_change(spark):

    df = create_df_method1(spark)

    # Use small number for unit testing
    df_new = increase_partitions(df, 2)

    assert get_partition_count(df_new) == 2



def test_masking(spark):

    df = create_df_method1(spark)

    result = add_masked_column(df, spark).collect()

    masked = result[0]["masked_card_number"]

    assert masked.endswith("4567")
    assert masked.startswith("************")