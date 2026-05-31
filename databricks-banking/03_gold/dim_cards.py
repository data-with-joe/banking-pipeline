# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

silver_table = f'{silver_schema}.cards'
disp_silver_table = f'{silver_schema}.disp'
target_table=f'{gold_schema}.dim_cards'

# COMMAND ----------

cards=spark.read.table(silver_table)
disp=spark.table(disp_silver_table)

# COMMAND ----------

joined_df = cards.join(disp, on='disp_id', how='left')

# COMMAND ----------

filter_df =joined_df.drop('ingestion_timestamp', 'source_file')

# COMMAND ----------

select_df=filter_df.select(
    F.col('disp_id'),
    F.col('card_id'),
    F.col('client_id'),
    F.col('account_id'),
    F.col('card_type'),
    F.col('type'),
    F.col('date_issued')
)

# COMMAND ----------

display(select_df)

# COMMAND ----------

(
    select_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(target_table)
)

# COMMAND ----------

