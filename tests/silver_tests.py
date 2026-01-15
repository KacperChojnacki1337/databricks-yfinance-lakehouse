import pyspark.sql.functions as F

def verify_silver_data(df):
    """
    Validates Silver data and RETURNS the message to be logged.
    """
    total_rows = df.count()
    if total_rows == 0:
        return False, "ERROR: DataFrame is empty."

    # --- 1. SMART SMA CHECK ---
    ticker_stats = df.groupBy("Ticket").agg(
        F.count("Date").alias("row_count"),
        F.count("SMA_200").alias("sma_count")
    )
    failed_sma = ticker_stats.filter((F.col("row_count") >= 200) & (F.col("sma_count") == 0)).collect()
    
    if len(failed_sma) > 0:
        bad_tickers = [row['Ticket'] for row in failed_sma]
        return False, f"ERROR: SMA_200 failed for: {bad_tickers}"

    # --- 2. ANOMALY DETECTION (The fix is here!) ---
    anomalies_df = df.filter(F.abs(F.col("Daily_Return_Pct")) > 200).select("Ticket", "Date", "Daily_Return_Pct")
    anomalies_count = anomalies_df.count()

    if anomalies_count > 0:
        sample = anomalies_df.limit(3).collect()
        details = ", ".join([f"{r['Ticket']} ({r['Date']}: {r['Daily_Return_Pct']}%)" for r in sample])
        # We return the message so it can be passed to log_execution
        warning_msg = f"SUCCESS with WARNING: Found {anomalies_count} anomalies (>200%). Examples: {details}"
        return True, warning_msg

    return True, "SUCCESS: All quality checks passed."