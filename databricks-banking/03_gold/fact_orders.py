# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

silver_table=f'{silver_schema}.orders'
target_table=f'{gold_schema}.fact_orders'

# COMMAND ----------

read_df=spark.table(silver_table)

# COMMAND ----------

final=read_df.withColumn('created_at', F.current_timestamp())

# COMMAND ----------

(
    final
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)

# COMMAND ----------

