from pyspark.sql import SparkSession
from src.Question2.util import *


def main():

    spark = SparkSession.builder \
        .appName("Assignment Question 2") \
        .getOrCreate()

    # Different read methods
    df1 = create_df_method1(spark)
    df2 = create_df_method2(spark)

    print("Method 1")
    df1.show()

    print("Method 2")
    df2.show()

    # Partitions
    original = get_partition_count(df1)
    print("Original partitions:", original)

    df_repart = increase_partitions(df1, 5)
    print("After repartition:", get_partition_count(df_repart))

    df_coalesce = decrease_partitions(df_repart, original)
    print("After coalesce:", get_partition_count(df_coalesce))

    # Masking
    final_df = add_masked_column(df1, spark)

    final_df.show()

    spark.stop()


if __name__ == "__main__":
    main()