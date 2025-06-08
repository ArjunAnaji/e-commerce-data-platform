from transform_landing_data_and_load_to_ods import (
    transform_customer,
    transform_salesorder,
    transform_salesorderitem
)
import pandas as pd
from unittest.mock import patch, MagicMock


# Test transformation of customer table
def test_transform_customer_valid():
    df = pd.DataFrame({
        "id": [1],
        "first_name": ["John"],
        "last_name": ["Doe"],
        "email": ["john@example.com"],
        "gender": ["male"],
        "shipping_address": ["123 Main Street"]
    })
    result = transform_customer(df)

    assert not result.empty  # Expect valid record to pass transformation
    assert result.iloc[0]["gender"] == "Male"  # Gender should be capitalized


# Test transformation of salesorder with valid customer_id and datetime fields
@patch("transform_landing_data_and_load_to_ods.pd.read_sql")
def test_transform_salesorder_valid(mock_read_sql):
    mock_read_sql.return_value = pd.DataFrame({"id": [1]})
    df = pd.DataFrame({
        "id": [1],
        "customer_id": [1],
        "order_number": ["ORD001"],
        "created_at": ["2024-01-01"],
        "modified_at": ["2024-01-02"],
        "order_total": [100.00],
        "total_qty_ordered": [2]
    })
    result = transform_salesorder(df, pg_engine=None)

    assert not result.empty  # Expect record to be valid
    assert pd.to_datetime(result.iloc[0]["created_at"])  # Ensure datetime is parsed


# Test transformation of salesorderitem including removal of product fields
@patch("transform_landing_data_and_load_to_ods.pd.read_sql")
@patch("transform_landing_data_and_load_to_ods.pd.DataFrame.to_sql")
def test_transform_salesorderitem_valid(mock_to_sql, mock_read_sql):
    # Mock read_sql for order_id and product_id checks
    mock_read_sql.side_effect = [
        pd.DataFrame({"id": [101]}),  # existing orders
        pd.DataFrame({"product_id": [1]})  # existing products
    ]

    # Sample input DataFrame
    df = pd.DataFrame({
        "item_id": [1],
        "order_id": [101],
        "created_at": ["2024-01-01"],
        "modified_at": ["2024-01-02"],
        "price": [10.00],
        "qty_ordered": [1],
        "line_total": [10.00],
        "product_id": [1],
        "product_sku": ["SKU001"],
        "product_name": ["Widget"]
    })

    # Use a MagicMock to simulate pg_engine
    mock_pg_engine = MagicMock()

    result = transform_salesorderitem(df, pg_engine=mock_pg_engine)

    # Check columns removed
    assert "product_sku" not in result.columns
    assert "product_name" not in result.columns

    # Verify product insertion attempted
    mock_to_sql.assert_called_once()
