from ..constant.app_constant import *


def get_df_from_file(spark, file_type, file_path):
    """
    :param spark:
    :param file_path:
    :param file_type:
    :return:
    """
    print("Inside get_df_from_file function")
    df = None

    if file_type == FILE_TYPE_CSV_CONS:
        print("Reading {} file".format(FILE_TYPE_CSV_CONS))
        df = spark.read.csv(file_path, header=True)

    print("Exiting out of get_df_from_file function")
    return df


def get_count_from_file(spark, file_type, file_path):
    print("Inside get_count_from_file function")

    df = get_df_from_file(spark, file_type, file_path)

    print()
    print(" ========= Printing df: ========= ")
    print(df)
    print()

    df_count = df.count()
    print("Exiting out of get_count_from_file function")
    return df_count


import pyspark.sql.functions as F


def get_filter_count_from_file(spark, file_type, file_path):
    print("Inside get_count_from_file function")

    df = get_df_from_file(spark, file_type, file_path)

    print()
    print(" ========= Printing df: ========= ")
    print(df)
    print()

    df = df.filter(F.col("first_name").isNotNull())

    df_count = df.count()
    print("Exiting out of get_count_from_file function")
    return df_count


def null_or_unknown_count(df):
    print("Inside null_or_unknown_count function")
    print(" ========================== ")
    print(df)
    print(" ========================== ")
    return df.sample(0.01).filter(
        F.col('env').isNull() | (F.col('env') == 'Unknown')
    ).count()