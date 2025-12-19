from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

def log_execution(spark, step_name, status, logs_path, message=None):
    # 1. Definiujemy schemat dla tabeli logów
    log_schema = StructType([
        StructField("step", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True)
    ])
    
    # 2. Tworzymy dane (ważne: message zamieniamy na pusty string jeśli jest None)
    log_data = [(step_name, datetime.now(), status, str(message) if message else "")]
    
    # 3. Tworzymy DataFrame z jawnym schematem
    log_df = spark.createDataFrame(log_data, schema=log_schema)
    
    # 4. Zapisujemy
    log_df.write.format("delta").mode("append").save(logs_path)