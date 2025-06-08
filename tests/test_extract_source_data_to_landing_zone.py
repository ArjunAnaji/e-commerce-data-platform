import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from extract_source_data_to_landing_zone import extract_incremental_data, load_to_landing


# Test customer incremental extract based on `id`
@patch("extract_source_data_to_landing_zone.pd.read_sql")
def test_extract_incremental_data_customer(mock_read_sql):
    mock_read_sql.side_effect = [
        pd.DataFrame({"id": [3]}),  # Last ID in PG
        pd.DataFrame({"id": [4, 5], "name": ["Test1", "Test2"]})  # New records from MySQL
    ]
    mysql = MagicMock()
    pg = MagicMock()
    df = extract_incremental_data(mysql, pg, "customer", "id")
    assert not df.empty  # Expect non-empty result since new rows are mocked
    assert len(df) == 2  # Expect exactly 2 new records returned


# Test salesorder incremental extract based on `created_at`
@patch("extract_source_data_to_landing_zone.pd.read_sql")
def test_extract_incremental_data_salesorder(mock_read_sql):
    mock_read_sql.side_effect = [
        pd.DataFrame({"created_at": ["2024-01-01 00:00:00"]}),
        pd.DataFrame({"id": [1], "created_at": ["2024-01-02 00:00:00"]})
    ]
    mysql = MagicMock()
    pg = MagicMock()
    df = extract_incremental_data(mysql, pg, "salesorder", "created_at")

    assert not df.empty  # Expect new rows to be returned
    assert "created_at" in df.columns  # Expect 'created_at' column to be included


# Test salesorderitem incremental extract based on `created_at`
@patch("extract_source_data_to_landing_zone.pd.read_sql")
def test_extract_incremental_data_salesorderitem(mock_read_sql):
    # First call to fetch last created_at value
    mock_read_sql.side_effect = [
        pd.DataFrame({"created_at": ["2024-01-01 00:00:00"]}),
        pd.DataFrame({
            "item_id": [1, 2],
            "order_id": [101, 102],
            "created_at": ["2024-01-02 00:00:00", "2024-01-02 12:00:00"]
        })
    ]

    mysql = MagicMock()
    pg = MagicMock()
    df = extract_incremental_data(mysql, pg, "salesorderitem", "created_at")

    assert not df.empty  # Should return 2 new rows
    assert "created_at" in df.columns  # Expect 'created_at' column to be included
    assert df.shape[0] == 2  # Expect exactly 2 records


# Test loading data to landing zone
@patch("extract_source_data_to_landing_zone.pd.DataFrame.to_sql")
def test_load_to_landing(mock_to_sql):
    df = pd.DataFrame({"a": [1, 2]})
    pg = MagicMock()
    load_to_landing(df, "some_table", pg)
    mock_to_sql.assert_called_once()  # Should call to_sql once to insert data
