# Databricks notebook source
# ── CATALOG CONFIG 
catalog       = "banking_project"
bronze_schema  = f"{catalog}.bronze"
silver_schema  = f"{catalog}.silver"
gold_schema    = f"{catalog}.gold"

# ── VOLUME PATH 
volume_path    = "/Volumes/banking_project/landing/raw_files"

# __ BRONZE TABLES 


# ── FILE PATHS 
ACCOUNTS_FILE      = f"{volume_path}/account.csv"
CLIENTS_FILE       = f"{volume_path}/client.csv"
TRANSACTIONS_FILE  = f"{volume_path}/trans.csv"
DISTRICTS_FILE     = f"{volume_path}/district.csv"
CARDS_FILE         = f"{volume_path}/card.csv"
LOANS_FILE         = f"{volume_path}/loan.csv"
DISPOSITIONS_FILE  = f"{volume_path}/disp.csv"
ORDERS_FILE        = f"{volume_path}/order.csv"

# COMMAND ----------

from pyspark.sql import functions as F
def add_metadata_columns(df):
    return(
        df
            .withColumn('ingestion_timestamp', F.current_timestamp())
            .withColumn('source_file', F.col('_metadata.file_path'))
    )

# COMMAND ----------

