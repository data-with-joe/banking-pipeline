# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

loan=f'{silver_schema}.loans'
target_table = f'{gold_schema}.dim_loans'

# COMMAND ----------

read_df=spark.table(loan)

# COMMAND ----------

(
    read_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(target_table)
)

# COMMAND ----------

