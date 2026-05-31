# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

orders = f'{bronze_schema}.orders'
orders_df = spark.table(orders)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC bank of the recipient
# MAGIC
# MAGIC
# MAGIC account_to
# MAGIC account of the recipient
# MAGIC
# MAGIC
# MAGIC amount
# MAGIC amount debited from order account
# MAGIC
# MAGIC
# MAGIC k_symbol
# MAGIC characterization of the payment

# COMMAND ----------

orders_renamed= orders_df.withColumnsRenamed({'bank_to':'recipient_bank','account_to':'recipient_account',
                               'amount':'order_amount', 'k_symbol':'payment_type'})

# COMMAND ----------

# MAGIC %md
# MAGIC SIPO Household Payment
# MAGIC UVER Loan Payment  
# MAGIC (empty) Unknown
# MAGIC POJISTNE Insurance Payment
# MAGIC LEASING Leasing Payment

# COMMAND ----------

orders_format = (orders_renamed
                 .withColumn('payment_type',
                              F.when(F.col('payment_type') == 'SIPO', 'Household Payment')
                              .when(F.col('payment_type') == 'UVER', 'Loan Payment')
                              .when(F.col('payment_type') == ' ', 'Unknown')
                              .when(F.col('payment_type') == 'POJISTNE', 'Insurance Payment')
                              .when(F.col('payment_type') == 'LEASING', 'Leasing Payment')
                              .otherwise('Unknown')
                              )
                 .dropDuplicates()
                 .filter(F.col('order_id').isNotNull())
                 
        
        )

# COMMAND ----------

display(orders_format)

# COMMAND ----------

(
    orders_format
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(f'{silver_schema}.orders')
)

# COMMAND ----------

