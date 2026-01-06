import pyspark.sql.functions as F

def verify_gold_data(df):
    """
    Validates Gold monthly aggregations.
    """
    total_records = df.count()
    if total_records == 0:
        return False, "ERROR: Gold DataFrame is empty."

    # 1. Check for illogical price relations
    bad_prices = df.filter(F.col("Monthly_Max_Close") < F.col("Monthly_Min_Close")).count()
    if bad_prices > 0:
        return False, f"ERROR: Found {bad_prices} rows where Max Price < Min Price."

    # 2. Return Sanity Check
    # Since we filtered Silver data, Monthly Avg Return should be within reasonable bounds
    extreme_monthly_returns = df.filter(F.abs(F.col("Monthly_Avg_Daily_Return_Pct")) > 50).count()
    if extreme_monthly_returns > 0:
        return True, f"SUCCESS with WARNING: {extreme_monthly_returns} tickers have avg monthly daily return > 50%."

    return True, f"SUCCESS: Aggregated {total_records} monthly records."