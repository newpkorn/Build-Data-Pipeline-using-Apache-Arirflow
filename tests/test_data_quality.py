import pandas as pd
import pytest

from utils.data_quality import check_nulls, check_duplicates


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