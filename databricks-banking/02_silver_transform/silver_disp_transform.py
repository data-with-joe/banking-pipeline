# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

disp = f'{bronze_schema}.disp'
disp_df = spark.read.table(disp)

# COMMAND ----------

disp_clean_df = (disp_df
                    .filter(F.col('disp_id').isNotNull() & 
                            F.col ('client_id').isNotNull())
                    .withColumn('type', F.initcap(F.col('type')))
                    .dropDuplicates()
                 
                 )

# COMMAND ----------

display(disp_clean_df)

# COMMAND ----------

disp_final_df= add_metadata_columns(disp_clean_df)

# COMMAND ----------

(
    disp_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(f'{silver_schema}.disp')
)

# COMMAND ----------

