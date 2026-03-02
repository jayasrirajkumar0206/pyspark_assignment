from pyspark.sql import SparkSession
from src.Question3.util import *


def main():

    spark = SparkSession.builder \
        .appName("Assignment Question 3") \
        .enableHiveSupport() \
        .getOrCreate()

    # Step 1
    df = create_log_df(spark)
    df.show()

    # Step 2
    df = rename_columns_dynamic(df)
    df.show()

    # Step 3
    print("Last 7 days activity")
    last_7_days_activity(df).show()

    # Step 4
    df = convert_login_date(df)
    df.show()

    # Step 5
    write_csv(df, "output/login_csv")

    # Step 6
    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    write_managed_table(df)

    spark.stop()


if __name__ == "__main__":
    main()