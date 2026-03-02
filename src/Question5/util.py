from pyspark.sql.types import *
from pyspark.sql.functions import col, avg, lower, current_date
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Diggibytes\spark\.venv\Scripts\python.exe"


# Dynamic schema generator
def generate_schema(columns):
    return StructType([StructField(c, StringType(), True) for c in columns])


# Create DataFrames
def create_employee_df(spark):

    data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("state", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("age", IntegerType(), True)
    ])

    return spark.createDataFrame(data, schema)


def create_department_df(spark):

    data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    return spark.createDataFrame(data, ["dept_id", "dept_name"])


def create_country_df(spark):

    data = [
        ("ny", "newyork"),
        ("ca", "california"),
        ("uk", "russia")
    ]

    return spark.createDataFrame(data, ["country_code", "country_name"])


# Avg salary
def avg_salary_department(emp_df):

    return emp_df.groupBy("department") \
        .agg(avg("salary").alias("avg_salary"))


# Employees name starts with m
def employees_m(emp_df, dept_df):

    return emp_df.join(
        dept_df,
        emp_df.department == dept_df.dept_id,
        "inner"
    ).filter(
        lower(col("employee_name")).startswith("m")
    ).select("employee_name", "dept_name")


# Bonus column
def add_bonus(emp_df):

    return emp_df.withColumn("bonus", col("salary") * 2)


# Reorder columns
def reorder_columns(emp_df):

    return emp_df.select(
        "employee_id",
        "employee_name",
        "salary",
        "state",
        "age",
        "department"
    )


# Join results
def join_results(emp_df, dept_df):

    inner_df = emp_df.join(
        dept_df,
        emp_df.department == dept_df.dept_id,
        "inner"
    )

    left_df = emp_df.join(
        dept_df,
        emp_df.department == dept_df.dept_id,
        "left"
    )

    right_df = emp_df.join(
        dept_df,
        emp_df.department == dept_df.dept_id,
        "right"
    )

    return inner_df, left_df, right_df


# Replace state with country
def country_replace(emp_df, country_df):

    return emp_df.join(
        country_df,
        emp_df.state == country_df.country_code,
        "left"
    ).select(
        "employee_id",
        "employee_name",
        "department",
        col("country_name").alias("state"),
        "salary",
        "age"
    )


# Lowercase + load_date
def lowercase_and_load(df):

    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    return df.withColumn("load_date", current_date())


# External tables

def write_external_tables(df, spark, path):

    spark.sql("CREATE DATABASE IF NOT EXISTS employee")

    df.write.mode("overwrite") \
        .option("path", f"{path}/parquet") \
        .format("parquet") \
        .saveAsTable("employee.employee_parquet")

    df.write.mode("overwrite") \
        .option("path", f"{path}/csv") \
        .option("header", True) \
        .format("csv") \
        .saveAsTable("employee.employee_csv")