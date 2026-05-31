# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

loans = f'{bronze_schema}.loans'
loans_df = spark.table(loans)

# COMMAND ----------

display(loans_df)

# COMMAND ----------

loan_df =loans_df.withColumn('date', F.to_date(
        F.concat(F.lit('19'), F.col('date').cast('string')), 
        'yyyyMMdd'
    )
)

# COMMAND ----------

from pyspark.sql.types import DateType, StringType, IntegerType, BooleanType
loans_renamed_df = loan_df.select(
    F.col('loan_id'),
    F.col('account_id'),
    F.col('date').cast(DateType()).alias('loan_date'),
    F.col('amount').alias('loan_amount'),
    F.col('duration').alias('loan_duration'),
    F.col('payments').cast(IntegerType()).alias('monthly_repayments'),
    F.col('status').alias('loan_status')

)

# COMMAND ----------

display(loans_renamed_df.select('loan_status').distinct())

# COMMAND ----------

loans_clean_df = (loans_renamed_df
    .withColumn('loan_status',
        F.when(F.col('loan_status') == 'A', 'Finished - No Issues')
        .when(F.col('loan_status') == 'B', 'Finished - Unpaid')
        .when(F.col('loan_status') == 'C', 'In Progress - Good Standing')
        .when(F.col('loan_status') == 'D', 'In Progress - In Debt')
        .otherwise('Unknown')
    )
    
)

# COMMAND ----------

display(loans_clean_df)

# COMMAND ----------

(
    loans_clean_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(f'{silver_schema}.loans')
)

# COMMAND ----------

