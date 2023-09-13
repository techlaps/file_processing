from unittest.mock import patch
from src.app.http_utils import parse

# src.app.http_utils.fetch_net

'''
@patch('src.app.http_utils.fetch_net')
def test_parse(mock_fetch_net):
    mock_fetch_net.return_value = "def"
    assert parse() == "def123"
'''


'''
@patch('src.app.http_utils.fetch_net', return_value = "def")
def test_parse(mock_fetch_net):
    assert parse() == "def123"'''


def test_parse():
    with patch('src.app.http_utils.fetch_net', return_value = "def"):
        assert parse() == "def123"
