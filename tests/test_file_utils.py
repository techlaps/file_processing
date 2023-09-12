import pytest
from os import path
from unittest import mock
import pyspark

from src.app.constant.app_constant import FILE_TYPE_CSV_CONS
from src.app.utils.file_utils import get_count_from_file

# python -m pytest tests/

# https://towardsdatascience.com/stop-mocking-me-unit-tests-in-pyspark-using-pythons-mock-library-a4b5cd019d7e

'''
def test_get_count_from_file(spark):
    file_location = "file:///home/techlaps/PycharmProjects/file_processing/tests/resources"
    customers_csv_file = "customers.csv"
    customers_json_file = "customers.json"
    file_path = path.join(file_location, customers_csv_file)
    print(file_path)

    df_count = get_count_from_file(spark,
                                   FILE_TYPE_CSV_CONS,
                                   file_path
                                   )
    print(f"Total records count: {df_count}")

    assert df_count == 2000
    '''


@mock.patch("src.app.utils.file_utils.get_df_from_file")
@mock.patch('pyspark.sql.DataFrame', spec=pyspark.sql.DataFrame)
def test_get_count_from_file(spark, mock_get_df_from_file, mock_df):
    file_location = "file:///home/techlaps/PycharmProjects/file_processing/tests/resources"
    customers_csv_file = "customers.csv"
    customers_json_file = "customers.json"
    file_path = path.join(file_location, customers_csv_file)
    print(file_path)

    mock_get_df_from_file.mock_df.count.return_value = 2000
    df_count = get_count_from_file(spark,
                                   FILE_TYPE_CSV_CONS,
                                   file_path
                                   )
    print(f"Total records count: {df_count}")

    assert df_count == 2000
