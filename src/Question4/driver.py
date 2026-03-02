from pyspark.sql import SparkSession
from src.Question4.util import *


def main():

    spark = SparkSession.builder \
        .appName("Assignment Question 4") \
        .enableHiveSupport() \
        .getOrCreate()

    # 1 Read JSON
    df = read_json_dynamic(spark, "data/employee.json")
    df.show()

    # 2 Flatten
    flat_df = flatten_df(df)

    # 3 Count difference
    print("Original count:", record_count(df))
    print("Flatten count:", record_count(flat_df))

    # 4 explode demos
    explode_demo(df, "skills").show()
    explode_outer_demo(df, "skills").show()
    posexplode_demo(df, "skills").show()

    # 5 filter
    flat_df = filter_id(flat_df)

    # 6 rename
    flat_df = rename_columns_snake(flat_df)

    # 7 load date
    flat_df = add_load_date(flat_df)

    # 8 partition columns
    flat_df = add_partition_columns(flat_df)

    flat_df.show()

    # 9 write
    write_partitioned_table(flat_df, spark)

    spark.stop()


if __name__ == "__main__":
    main()