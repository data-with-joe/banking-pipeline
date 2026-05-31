# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

districts_read=f'{silver_schema}.districts'
target_table= f'{gold_schema}.dim_districts'

# COMMAND ----------

districts_df=spark.table(districts_read)

# COMMAND ----------

districts_df.display()

# COMMAND ----------

(
    districts_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)

# COMMAND ----------

