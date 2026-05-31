# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

cards = f'{bronze_schema}.cards'
cards_df = spark.table(cards)

# COMMAND ----------

cards_df.printSchema()

# COMMAND ----------

cards_dropped = cards_df.dropDuplicates().filter(F.col('card_id').isNotNull())


# COMMAND ----------

cards_renamed = (
        cards_dropped
            .withColumnsRenamed({'type':'card_type', 'issued': 'date_issued'})
)

# COMMAND ----------


cards_format = (cards_renamed
                .withColumn('date_issued',   F.to_date(F.col('date_issued'), 'yyMMdd HH:mm:ss').cast('date'))
                .withColumn('card_type', F.initcap(F.col('card_type')))


)


# COMMAND ----------

cards_final_df = add_metadata_columns(cards_format)

# COMMAND ----------

display(cards_final_df)

# COMMAND ----------

(
    cards_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(f'{silver_schema}.cards')
)

# COMMAND ----------

display(spark.table(f'{silver_schema}.cards'))

# COMMAND ----------

