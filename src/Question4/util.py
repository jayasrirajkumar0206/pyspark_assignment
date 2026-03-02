from pyspark.sql.functions import col, explode, explode_outer, posexplode, current_date
from pyspark.sql.types import *
import re
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"



# Read JSON dynamically

def read_json_dynamic(spark, path):
    return spark.read.option("multiline", True).json(path)


# Flatten JSON (generic)
def flatten_df(df):

    flat_cols = [c[0] for c in df.dtypes if c[1] != 'array' and not c[1].startswith('struct')]
    nested_cols = [c[0] for c in df.dtypes if c[1].startswith('struct')]
    array_cols = [c[0] for c in df.dtypes if c[1].startswith('array')]

    # Flatten struct
    while len(nested_cols) > 0:
        col_name = nested_cols.pop()
        expanded = [col(col_name + '.' + k).alias(col_name + '_' + k)
                    for k in df.select(col_name + '.*').columns]

        df = df.select("*", *expanded).drop(col_name)

        nested_cols = [c[0] for c in df.dtypes if c[1].startswith('struct')]

    # Explode arrays
    for col_name in array_cols:
        df = df.withColumn(col_name, explode_outer(col(col_name)))

    return df


# Count difference
def record_count(df):
    return df.count()



# explode comparison
def explode_demo(df, column):
    return df.select(explode(col(column)))


def explode_outer_demo(df, column):
    return df.select(explode_outer(col(column)))


def posexplode_demo(df, column):
    return df.select(posexplode(col(column)))


# Filter id
def filter_id(df):
    return df.filter(col("id") == "0001")


# Camel → snake case
def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def rename_columns_snake(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, camel_to_snake(c))
    return df


# Add load_date
def add_load_date(df):
    return df.withColumn("load_date", current_date())


# Create year, month, day
def add_partition_columns(df):
    return df \
        .withColumn("year", col("load_date").substr(1, 4)) \
        .withColumn("month", col("load_date").substr(6, 2)) \
        .withColumn("day", col("load_date").substr(9, 2))


# Write partitioned table
def write_partitioned_table(df, spark):

    spark.sql("CREATE DATABASE IF NOT EXISTS employee")

    df.write \
        .format("json") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .option("replaceWhere", "year IS NOT NULL") \
        .saveAsTable("employee.employee_details")