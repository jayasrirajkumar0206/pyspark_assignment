from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F

import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"


# Schema

def get_schema():
    return StructType([
        StructField("card_number", StringType(), True)
    ])


# Different read methods

def create_df_method1(spark):
    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]
    return spark.createDataFrame(data, get_schema())


def create_df_method2(spark):
    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]
    return spark.createDataFrame(data).toDF("card_number")



# Partitions

def get_partition_count(df):
    return df.rdd.getNumPartitions()


def increase_partitions(df, num):
    return df.repartition(num)


def decrease_partitions(df, num):
    return df.coalesce(num)



# UDF for masking
def mask_card(card):
    if card:
        return "*" * (len(card) - 4) + card[-4:]
    return None


def register_udf(spark):
    return udf(mask_card, StringType())



# Final output

def add_masked_column(df, spark):

    mask_udf = register_udf(spark)

    return df.withColumn(
        "masked_card_number",
        mask_udf(F.col("card_number"))
    )