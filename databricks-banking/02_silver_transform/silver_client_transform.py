# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

clients = f'{bronze_schema}.clients'
clients_df = spark.table(clients)

# COMMAND ----------

clients_silver_df = (clients_df
    .withColumn('birth_month_raw', 
        (F.col('birth_number') / 100).cast('int') % 100
    )
    .withColumn('gender',
        F.when(F.col('birth_month_raw') > 50, 'Female')
        .otherwise('Male')
    )
    .withColumn('birth_month',
        F.when(F.col('birth_month_raw') > 50, F.col('birth_month_raw') - 50)
        .otherwise(F.col('birth_month_raw'))
    )
    .withColumn('birth_year',
        (F.col('birth_number') / 10000).cast('int') + 1900
    )
    .withColumn('birth_day',
        F.col('birth_number') % 100
    )
    .drop('birth_month_raw')
)

# COMMAND ----------

display(clients_silver_df)

# COMMAND ----------

clients_final_df = add_metadata_columns(clients_silver_df)

# COMMAND ----------

changes = clients_final_df.withColumn('birth_date', 
    F.to_date(
        F.concat(
            F.col('birth_year').cast('string'),
            F.lpad(F.col('birth_month').cast('string'), 2, '0'),
            F.lpad(F.col('birth_day').cast('string'), 2, '0')
        ), 
        'yyyyMMdd'
    )
).drop('birth_number', 'birth_year', 'birth_month', 'birth_day')

display(changes)

# COMMAND ----------

(
    changes
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(f'{silver_schema}.clients')
)

# COMMAND ----------

