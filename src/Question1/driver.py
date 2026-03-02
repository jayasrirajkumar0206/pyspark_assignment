from pyspark.sql import SparkSession
from src.Question1.util import *
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"

def main():

    spark = SparkSession.builder \
        .appName("Assignment Question 1") \
        .getOrCreate()

    # Create DataFrames
    purchase_df = create_purchase_df(spark)
    product_df = create_product_df(spark)

    print("Purchase Data")
    purchase_df.show()

    print("Product Data")
    product_df.show()

    print("Customers only iphone13")
    customers_only_iphone13(purchase_df).show()

    print("Upgraded customers")
    upgraded_customers(purchase_df).show()

    print("Customers bought all products")
    customers_all_products(purchase_df, product_df).show()

    spark.stop()


if __name__ == "__main__":
    main()