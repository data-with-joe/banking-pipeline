# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

trans=f'{bronze_schema}.trans'
trans_df = spark.table(trans)

# COMMAND ----------

display(trans_df)

# COMMAND ----------

dropped_trans = (trans_df
                 .fillna({'k_symbol': 'Unknown', 'bank': 'Unknown', 'operation':'Unknown','account': '0'})
                 .dropDuplicates()
                 .withColumn('type', F.when(F.col('type') == 'PRIJEM', 'Credit')
                             .when(F.col('type') == 'VYDAJ', 'Debit')
                             .otherwise('Unknown'))
                 .withColumn('operation', 
                             F.when(F.col('operation') == 'PREVOD NA UCET','Transfer to Account' )
                             .when(F.col('operation') == 'PREVOD Z UCTU', 'Transfer to Account')
                             .otherwise('Unknown'))
                 .withColumnRenamed('k_symbol', 'payment_type')
                 .withColumn('payment_type',
                              F.when(F.col('payment_type') == 'SIPO', 'Household Payment')
                              .when(F.col('payment_type') == 'UVER', 'Loan Payment')
                              .when(F.col('payment_type') == ' ', 'Unknown')
                              .when(F.col('payment_type') == 'POJISTNE', 'Insurance Payment')
                              .when(F.col('payment_type') == 'DUCHOD', 'Pension Payment')
                              .otherwise('Unknown')
                              )
                 .withColumn('date', F.to_date(F.concat(F.lit('19'), F.col('date')
                                        .cast('string')), 'yyyyMMdd')
            )
)

# COMMAND ----------

display(dropped_trans)

# COMMAND ----------

print(f"Before drop: {trans_df.count()}")
print(f"After drop: {dropped_trans.count()}")

# COMMAND ----------

(
    dropped_trans
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(f'{silver_schema}.transactions')
)

# COMMAND ----------

