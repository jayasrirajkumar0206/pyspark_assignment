from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp, current_date, date_sub, count, to_date

import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"


# Schema creation

def get_schema():
    return StructType([
        StructField("log id", IntegerType(), True),
        StructField("user$id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])



# Create DataFrame

def create_log_df(spark):

    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]

    return spark.createDataFrame(data, get_schema())



# Dynamic column rename

def rename_columns_dynamic(df):

    new_cols = ["log_id", "user_id", "user_activity", "time_stamp"]

    for old, new in zip(df.columns, new_cols):
        df = df.withColumnRenamed(old, new)

    return df



# Last 7 days activity

def last_7_days_activity(df):

    df = df.withColumn(
        "time_stamp",
        to_timestamp(col("time_stamp"))
    )

    return df.filter(
        col("time_stamp") >= date_sub(current_date(), 7)
    ).groupBy("user_id") \
        .agg(count("*").alias("action_count"))


# Convert timestamp → login_date
def convert_login_date(df):

    return df.withColumn(
        "login_date",
        to_date(col("time_stamp"))
    )



# Write CSV with options
def write_csv(df, path):

    df.write \
        .mode("overwrite") \
        .option("header", True) \
        .option("delimiter", ",") \
        .option("compression", "gzip") \
        .csv(path)


# Managed table
def write_managed_table(df):

    df.write \
        .mode("overwrite") \
        .saveAsTable("user.login_details")