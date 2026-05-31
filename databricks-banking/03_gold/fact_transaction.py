# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

silver_Table=f'{silver_schema}.transactions'
silver_Table_disp=f'{silver_schema}.disp'
target_table=f'{gold_schema}.fact_transactions'

# COMMAND ----------

silver_Table_read= spark.read.table(silver_Table)
silver_disp= spark.table(silver_Table_disp)

# COMMAND ----------

joined_df = silver_Table_read.join(silver_disp, on='account_id', how='left')

# COMMAND ----------

dropped_df= joined_df.select(
   F.col('account_id'),
   F.col('trans_id'),
   F.col('client_id'),
   F.col('disp_id'),
   F.col('account'),
   F.col('amount'),
   F.col('balance'),
   F.col('payment_type'),
   F.col('bank'),
   F.col('operation'),
   F.col('transactions.type'),
   F.col('date')



).withColumn('created_at', F.current_timestamp())
display(dropped_df)

# COMMAND ----------

(
    dropped_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', True)
        .saveAsTable(target_table)
)

# COMMAND ----------

