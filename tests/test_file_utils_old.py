import pytest
from os import path
from unittest.mock import patch, MagicMock
import pyspark
from os.path import join
import pandas as pd

import sys
from os.path import dirname
print(dirname(__file__))
tests_dir_path = dirname(__file__)
tests_dir_path_list = tests_dir_path.split('/')[0:-1]
tests_dir_path_list.append("src")
#tests_dir_path_list.append("app")
app_dir_path = "/" + join(*tests_dir_path_list)
print(app_dir_path)
sys.path.append(app_dir_path)
print(sys.path)
#sys.path.remove(app_dir_path)

print(sys.path)

from app.constant.app_constant import FILE_TYPE_CSV_CONS
from app.utils.file_utils import get_count_from_file, \
    null_or_unknown_count, \
    get_filter_count_from_file
# app.utils.file_utils.get_df_from_file

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


"""
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

    assert df_count == 2000"""

'''
# Today - 2023-09-13 - Working
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

#from app.utils.file_utils import get_df_from_file
print("Debug")
#@patch('pyspark.sql.DataFrame', spec=pyspark.sql.DataFrame)
@patch('app.utils.file_utils.get_df_from_file')
#def test_get_count_from_file_with_mock(mock_df, mock_get_df_from_file, spark):
def test_get_count_from_file_with_mock(mock_get_df_from_file, spark):
    file_location = "file:///home/techlaps/PycharmProjects/file_processing/tests/resources"
    customers_csv_file = "customers.csv"
    customers_json_file = "customers.json"
    file_path = path.join(file_location, customers_csv_file)
    print(file_path)

    #mock_df = MagicMock # not working
    #mock_df.count.return_value = 2000
    from pyspark.sql.types import _parse_datatype_string
    # schema= _parse_datatype_string("id int")

    data = {"id": [i for i in range(2000)]}
    pd_df = pd.DataFrame(data)
    print("Before creating dataframe")
    df = spark.createDataFrame(pd_df)
    print("Before assigning dataframe")
    #df.show()
    mock_get_df_from_file.return_value = df
    print("After assigning dataframe")
    #df.show()

    df_count = get_count_from_file(spark,
                                   FILE_TYPE_CSV_CONS,
                                   file_path
                                   )
    print(f"Total records count: {df_count}")
    print(" ***************************** \n ***************************** ")
    assert df_count == 2000


@patch("pyspark.sql.functions.col")
@patch('pyspark.sql.DataFrame', spec=pyspark.sql.DataFrame)
@patch('app.utils.file_utils.get_df_from_file')
def test_get_filter_count_from_file(mock_get_df_from_file,
        mock_df, mock_col_function, spark):
    file_location = "file:///home/techlaps/PycharmProjects/file_processing/tests/resources"
    customers_csv_file = "customers.csv"
    customers_json_file = "customers.json"
    file_path = path.join(file_location, customers_csv_file)
    print(file_path)

    print("Before using mock_functions - 1")
    mock_col_function.isNotNull.return_value = True  # (or False also works)
    print("Before using mock_df - 1")
    mock_df.filter(ANY).count.return_value = 2000
    print("After using mock_df - 1")

    print("Before assigning dataframe")
    mock_get_df_from_file.return_value = mock_df
    print("After assigning dataframe")

    df_count = get_filter_count_from_file(spark,
                                   FILE_TYPE_CSV_CONS,
                                   file_path
                                   )
    print(f"Total records count - 1: {df_count}")
    print(" #################### \n ################# ")

    assert df_count == 2000


'''
import pyspark.sql.functions as F
import pandas as pd
data = {"env": ["env" + str(i) for i in range(2000)]}
pd_df = pd.DataFrame(data)
print("Before creating dataframe")
df = spark.createDataFrame(pd_df)
'''
from unittest.mock import ANY


@patch('pyspark.sql.functions.col')
@patch('pyspark.sql.DataFrame', spec=pyspark.sql.DataFrame)
def test_null_or_unknown_validation(mock_df, mock_functions):
    print("Before using mock_functions")
    mock_functions.isNull.return_value = True # (or False also works)
    print("Before using mock_df")
    mock_df.sample(0.01).filter(ANY).count.return_value = 250
    print("After using mock_df")
    #mock_df.sample().filter().count.return_value = 250

    print("Before calling null_or_unknown_count")
    count = null_or_unknown_count(mock_df)
    print("After calling null_or_unknown_count")
    print(f"Total records count: {count}")

    assert count == 250