import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from utils.data_quality import check_nulls, check_duplicates, check_null_rates_for_date


def test_check_nulls_pass():
    """
    Test that dataset without null values passes the check.
    """
    df = pd.DataFrame({
        "currency": ["USD", "EUR", "THB"],
        "rate": [36.5, 0.92, 1.0]
    })

    result = check_nulls(df)

    assert result is True


def test_check_nulls_fail():
    """
    Test that dataset with null values fails the check.
    """
    df = pd.DataFrame({
        "currency": ["USD", None, "THB"],
        "rate": [36.5, 0.92, 1.0]
    })

    result = check_nulls(df)

    assert result is False


def test_check_duplicates_pass():
    """
    Test dataset without duplicate rows.
    """
    df = pd.DataFrame({
        "currency": ["USD", "EUR", "THB"],
        "rate": [36.5, 0.92, 1.0]
    })

    result = check_duplicates(df)

    assert result is True


def test_check_duplicates_fail():
    """
    Test dataset with duplicate rows.
    """
    df = pd.DataFrame({
        "currency": ["USD", "USD", "THB"],
        "rate": [36.5, 36.5, 1.0]
    })

    result = check_duplicates(df)

    assert result is False


@patch('utils.data_quality.MySqlHook')
def test_check_null_rates_for_date_pass(mock_mysql_hook):
    """
    Test that the check passes when no null rates are found.
    """
    # Arrange
    mock_df = pd.DataFrame({"rate": [1.0, 1.1, 1.2]})
    mock_instance = MagicMock()
    mock_instance.get_pandas_df.return_value = mock_df
    mock_mysql_hook.return_value = mock_instance

    # Act & Assert
    # The function should not raise an exception
    try:
        check_null_rates_for_date("some_table", "some_conn", "2024-01-01")
    except ValueError:
        pytest.fail("check_null_rates_for_date raised ValueError unexpectedly.")


@patch('utils.data_quality.MySqlHook')
def test_check_null_rates_for_date_fail(mock_mysql_hook):
    """
    Test that the check fails (raises ValueError) when null rates are found.
    """
    # Arrange
    mock_df = pd.DataFrame({"rate": [1.0, None, 1.2]})
    mock_instance = MagicMock()
    mock_instance.get_pandas_df.return_value = mock_df
    mock_mysql_hook.return_value = mock_instance

    # Act & Assert
    with pytest.raises(ValueError, match="Data quality check failed: Null rates found"):
        check_null_rates_for_date("some_table", "some_conn", "2024-01-01")


@patch('utils.data_quality.MySqlHook')
def test_check_null_rates_for_date_empty_df(mock_mysql_hook):
    """
    Test that the check passes without error for an empty DataFrame.
    """
    # Arrange
    mock_df = pd.DataFrame({"rate": []})
    mock_instance = MagicMock()
    mock_instance.get_pandas_df.return_value = mock_df
    mock_mysql_hook.return_value = mock_instance

    # Act & Assert
    try:
        check_null_rates_for_date("some_table", "some_conn", "2024-01-01")
    except ValueError:
        pytest.fail("check_null_rates_for_date raised ValueError unexpectedly on empty DataFrame.")