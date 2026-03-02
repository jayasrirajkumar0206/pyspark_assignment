from pyspark.sql import SparkSession
from src.Question5.util import *


def main():

    spark = SparkSession.builder \
        .appName("Assignment Question 5") \
        .enableHiveSupport() \
        .getOrCreate()

    emp_df = create_employee_df(spark)
    dept_df = create_department_df(spark)
    country_df = create_country_df(spark)

    avg_salary_department(emp_df).show()

    employees_m(emp_df, dept_df).show()

    emp_df = add_bonus(emp_df)
    emp_df.show()

    reorder_columns(emp_df).show()

    inner_df, left_df, right_df = join_results(emp_df, dept_df)

    inner_df.show()
    left_df.show()
    right_df.show()

    country_df_new = country_replace(emp_df, country_df)

    final_df = lowercase_and_load(country_df_new)

    write_external_tables(final_df, spark, "output/external")

    spark.stop()


if __name__ == "__main__":
    main()