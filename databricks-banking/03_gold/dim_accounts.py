# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

accounts_silver_table= f'{silver_schema}.accounts'
target_table = f'{gold_schema}.dim_accounts'

# COMMAND ----------

accounts_read =spark.table(accounts_silver_table)

# COMMAND ----------

accounts_final= accounts_read.drop('ingestion_timestamp', 'source_file')

# COMMAND ----------

(
    accounts_final
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)

# COMMAND ----------

