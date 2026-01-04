import pyspark.sql.functions as F

def verify_bronze_batch(df, ticker_name):
    """
    Validates the data packet before saving. 
    Returns (bool, message)
    """
    # 1. Checking for an empty DF
    if df.count() == 0:
        return False, f"Pusty DataFrame dla {ticker_name}"
    
    # 2. Checking for critical NULLs
    null_count = df.filter(F.col("Date").isNull() | F.col("Close").isNull()).count()
    if null_count > 0:
        return False, f"Znaleziono {null_count} NULLi w Date/Close dla {ticker_name}"

    return True, "OK"