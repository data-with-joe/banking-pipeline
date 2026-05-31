# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC 1.  Create notebook 01_bronze_ingestion
# MAGIC  2. Read each CSV file from the Volume path into a Spark DataFrame
# MAGIC  3. Write each one as a raw Delta table into a bronze schema

# COMMAND ----------

# MAGIC %run ../configs/config

# COMMAND ----------

accounts_df      = f"{volume_path}/account.csv"
clients_df       = f"{volume_path}/client.csv"
trans_df         = f"{volume_path}/trans.csv"
districts_df      = f"{volume_path}/district.csv"
cards_df         = f"{volume_path}/card.csv"
loans_df         = f"{volume_path}/loan.csv"
disp_df          = f"{volume_path}/disp.csv"
orders_df        = f"{volume_path}/order.csv"

# COMMAND ----------

accounts_read_df =(
        spark.read.format('csv')
            .option('header', True)
            .option('inferSchema', True)
            .option('delimiter', ';')
            .load(accounts_df)

)

cards_read_df =(
        spark.read.format('csv')
            .option('header', True)
            .option('inferSchema', True)
            .option('delimiter', ';')
            .load(cards_df)

)

clients_read_df =(
        spark.read.format('csv')
            .option('header', True)
            .option('inferSchema', True)
            .option('delimiter', ';')
            .load(clients_df)

)

disp_read_df =(
        spark.read.format('csv')
            .option('header', True)
            .option('inferSchema', True)
            .option('delimiter', ';')
            .load(disp_df)

)

districts_read_df =(
        spark.read.format('csv')
            .option('header', True)
            .option('inferSchema', True)
            .option('delimiter', ';')
            .load(districts_df)

)

loans_read_df =(
        spark.read.format('csv')
            .option('header', True)
            .option('inferSchema', True)
            .option('delimiter', ';')
            .load(loans_df)

)

orders_read_df =(
        spark.read.format('csv')
            .option('header', True)
            .option('inferSchema', True)
            .option('delimiter', ';')
            .load(orders_df)

)

trans_read_df =(
        spark.read.format('csv')
            .option('header', True)
            .option('inferSchema', True)
            .option('delimiter', ';')
            .load(trans_df)

)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS banking_project.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS banking_project.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS banking_project.gold")


# COMMAND ----------


spark.sql("use catalog banking_project")


# COMMAND ----------

(
     accounts_read_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable('bronze.accounts')
)

(
     cards_read_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable('bronze.cards')
)

(
     clients_read_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable('bronze.clients')
)

(
     disp_read_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable('bronze.disp')
)

(
     districts_read_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable('bronze.districts')
)

(
     loans_read_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable('bronze.loans')
)

(
     orders_read_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable('bronze.orders')
)

(
     trans_read_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable('bronze.trans')
)

# COMMAND ----------

display(spark.table('bronze.trans'))


# COMMAND ----------

