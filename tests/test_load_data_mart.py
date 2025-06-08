import pandas as pd
from load_data_mart import apply_data_quality_checks


# Test apply_data_quality_checks with invalid rows
def test_apply_data_quality_checks():
    df = pd.DataFrame({
        "item_id": [1, 1],
        "order_id": [100, 100],
        "order_total": [200.0, 0],  # One valid, one zero
        "total_qty_ordered": [1, 0],
        "item_price": [10.0, -5],
        "item_qty_order": [2, -1],
        "item_unit_total": [20.0, 0],
        "order_created_at": [pd.Timestamp("2023-01-01"), pd.Timestamp("2030-01-01")],  # One future
        "customer_email": ["valid@domain.com", "test@tempmail.com"],  # One disposable domain
        "customer_name": ["John Doe", ""]  # One blank name
    })

    cleaned_df = apply_data_quality_checks(df)

    assert len(cleaned_df) == 1  # Only one record should survive all checks
    assert cleaned_df.iloc[0]["customer_email"] != "test@tempmail.com"  # Disposable email should be removed
    assert cleaned_df.iloc[0]["customer_name"].strip() != ""  # Blank name should be removed
    assert cleaned_df.iloc[0]["order_created_at"] <= pd.Timestamp.now()  # No future dates allowed
