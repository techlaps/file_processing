from pyspark.sql import SparkSession
from app.utils.file_utils import *
from app.constant.app_constant import *
from os import path


def run(spark):
    file_path = "/home/techlaps/PycharmProjects/file_processing/src/tests/resources"

    customers_csv_file = "customers.csv"
    customers_json_file = "customers.json"

    df_count = get_count_from_file(spark,
                                   FILE_TYPE_CSV_CONS,
                                   path.join(file_path, customers_csv_file))
    print(f"Total records count: {df_count}")


if __name__ == "__main__":
    print("Application Started ...")
    spark = (SparkSession
             .builder
             .appName("File Processing Application")
             .getOrCreate())

    run(spark)
    print("Application Completed.")