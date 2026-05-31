# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

silver_clients_table = f'{silver_schema}.clients'
silver_disp_table = f'{silver_schema}.disp'
target_table=f'{gold_schema}.clients_dim'

# COMMAND ----------

read_client= spark.table(silver_clients_table)
read_disp = spark.table(silver_disp_table)

# COMMAND ----------

clients_join_df =( read_client 
                    .join(read_disp, on='client_id',how='left')
                  )

# COMMAND ----------

clients_drop=clients_join_df.drop('ingestion_timestamp','source_file')


# COMMAND ----------

clients_select=clients_drop.select(
    F.col('client_id'),
    F.col('district_id'),
    F.col('disp_id'),
    F.col('account_id'),
    F.col('gender'),
    F.col('type'),
    F.col('birth_date')
)
display(clients_select)

# COMMAND ----------

(
    clients_select
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)

# COMMAND ----------

