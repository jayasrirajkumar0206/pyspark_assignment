from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import collect_set, array_contains, size
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"


# Schema Creation

def get_purchase_schema():
    return StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
    ])


def get_product_schema():
    return StructType([
        StructField("product_model", StringType(), True)
    ])



# Data Creation

def create_purchase_df(spark):

    data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13")
    ]

    return spark.createDataFrame(data, get_purchase_schema())


def create_product_df(spark):

    data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
    ]

    return spark.createDataFrame(data, get_product_schema())



# Q2 Customers only iphone13

def customers_only_iphone13(purchase_df):

    agg_df = purchase_df.groupBy("customer") \
        .agg(collect_set("product_model").alias("products"))

    return agg_df.filter(
        (size("products") == 1) &
        array_contains("products", "iphone13")
    ).select("customer")



# Q3 Upgraded customers
def upgraded_customers(purchase_df):

    agg_df = purchase_df.groupBy("customer") \
        .agg(collect_set("product_model").alias("products"))

    return agg_df.filter(
        array_contains("products", "iphone13") &
        array_contains("products", "iphone14")
    ).select("customer")



# Q4 Bought all products

def customers_all_products(purchase_df, product_df):

    total_products = product_df.count()

    agg_df = purchase_df.groupBy("customer") \
        .agg(collect_set("product_model").alias("products"))

    return agg_df.filter(
        size("products") == total_products
    ).select("customer")