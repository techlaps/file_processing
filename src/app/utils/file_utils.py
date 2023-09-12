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
    df_count = df.count()
    print("Exiting out of get_count_from_file function")
    return df_count