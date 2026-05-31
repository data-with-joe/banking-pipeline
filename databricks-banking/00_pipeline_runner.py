# Databricks notebook source
# MAGIC %run /Workspace/Users/aladejoseph656@gmail.com/databricks-banking/01_bronze_ingestion/01_bronze_ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC #SILVER

# COMMAND ----------

# MAGIC %run /Workspace/Users/aladejoseph656@gmail.com/databricks-banking/02_silver_transform/silver_accounts_transform
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/aladejoseph656@gmail.com/databricks-banking/02_silver_transform/silver_cards_transform
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/aladejoseph656@gmail.com/databricks-banking/02_silver_transform/silver_client_transform
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/aladejoseph656@gmail.com/databricks-banking/02_silver_transform/silver_disp_transform
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/aladejoseph656@gmail.com/databricks-banking/02_silver_transform/silver_district_transform

# COMMAND ----------

# MAGIC %run /Workspace/Users/aladejoseph656@gmail.com/databricks-banking/02_silver_transform/silver_loans_transform
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/aladejoseph656@gmail.com/databricks-banking/02_silver_transform/silver_orders_transform
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/aladejoseph656@gmail.com/databricks-banking/02_silver_transform/silver_trans_transform

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #GOLD
# MAGIC

# COMMAND ----------

# MAGIC %run ./03_gold/dim_accounts

# COMMAND ----------

# MAGIC %run ./03_gold/dim_cards

# COMMAND ----------

# MAGIC %run ./03_gold/dim_clients

# COMMAND ----------

# MAGIC %run ./03_gold/dim_districts

# COMMAND ----------

# MAGIC %run ./03_gold/dim_loans

# COMMAND ----------

# MAGIC %run ./03_gold/fact_orders

# COMMAND ----------

# MAGIC %run ./03_gold/fact_transaction

# COMMAND ----------

