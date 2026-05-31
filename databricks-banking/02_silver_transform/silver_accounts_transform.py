# Databricks notebook source
# MAGIC %run ../configs/config
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql('use catalog banking_project')

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

accounts_df = f'{bronze_schema}.accounts'
accounts_read_df = spark.table(accounts_df)

# COMMAND ----------

accounts_dropped_df = accounts_read_df.dropDuplicates()

# COMMAND ----------


accounts_null_df = accounts_dropped_df.filter(
    F.col('account_id').isNotNull()
)


# COMMAND ----------

accounts_translate_df = (accounts_null_df
                         .withColumn('frequency',
                            F.when(F.col('frequency') == 'POPLATEK MESICNE','Monthly')
                            .when(F.col('frequency') == 'POPLATEK TYDNE', 'Weekly')
                            .when(F.col('frequency') == 'POPLATEK PO OBRATU',
                                'After Each Transaction')
                            .otherwise('Unknown')
)
                         
    ).withColumn('date', F.to_date(F.concat(F.lit('19'), F.col('date').cast('string')), 'yyyyMMdd')
        ).withColumnRenamed('date', 'opened_date')

# COMMAND ----------

accounts_final_df = add_metadata_columns(accounts_translate_df)

# COMMAND ----------

(
    accounts_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(f'{silver_schema}.accounts')
)

# COMMAND ----------

display(spark.table(f'{silver_schema}.accounts'))

# COMMAND ----------

