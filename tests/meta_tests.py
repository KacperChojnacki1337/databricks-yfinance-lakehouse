import pyspark.sql.functions as F

def verify_metadata_batch(df):
    """
    Validating the ticket list from Google Drive before updating the SQL/Meta table.
    """
    # 1. Checking if anything arrived at all
    count = df.count()
    if count == 0:
        return False, "The metadata file is empty"
    
    # 2. Checking if we have the required columns
    required_columns = ["company_name", "ticket"]
    for col in required_columns:
        if col not in df.columns:
            return False, f"Required column missing: {col}"

    # 3. Checking if tickets are not empty (critical for JOINs)
    null_tickets = df.filter(F.col("ticket").isNull() | (F.col("ticket") == "")).count()
    if null_tickets > 0:
        return False, f"{null_tickets} records found without a ticker"

    return True, "OK"