from src.app.random_utils import get_random_number
from src.app.utilize_random_utils import get_number
from unittest.mock import patch

'''
def test_get_number():
    assert get_number(3) == "Number is match"
'''


@patch("src.app.random_utils.get_random_number", return_value = 3)
def test_get_number(mock_get_random_number):
    #mock_get_random_number.return_value = 3
    assert get_number(3) == True