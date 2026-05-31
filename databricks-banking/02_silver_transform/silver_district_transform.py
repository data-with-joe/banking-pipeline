# Databricks notebook source
# MAGIC %run ../configs/config

# COMMAND ----------

district = f'{bronze_schema}.districts'
district_df = spark.table(district)

# COMMAND ----------

display(district_df)

# COMMAND ----------

# MAGIC %md
# MAGIC A1
# MAGIC district id
# MAGIC
# MAGIC A2
# MAGIC district name
# MAGIC
# MAGIC A3
# MAGIC region
# MAGIC
# MAGIC A4
# MAGIC no. of inhabitants
# MAGIC
# MAGIC A5
# MAGIC no. of municipalities w/ inhabitants < 499
# MAGIC
# MAGIC A6
# MAGIC no. of municipalities w/ inhabitants 500 - 1999
# MAGIC
# MAGIC A7
# MAGIC no. of municipalities w/ inhabitants 2000 - 9999
# MAGIC
# MAGIC A8
# MAGIC no. of municipalities w/ inhabitants > 10000
# MAGIC
# MAGIC A9
# MAGIC no. of cities
# MAGIC
# MAGIC A10
# MAGIC ration of urban inhabitants
# MAGIC
# MAGIC A11
# MAGIC average salary
# MAGIC
# MAGIC A12
# MAGIC unemployment rate in 1995
# MAGIC
# MAGIC A13
# MAGIC unemployment rate in 1996
# MAGIC
# MAGIC A14
# MAGIC no. of enterpreneurs per 1000 inhabitants
# MAGIC
# MAGIC A15
# MAGIC no. of crimes commited in 1995
# MAGIC
# MAGIC A16
# MAGIC no. of crimes commited in 1996
# MAGIC

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, FloatType, DoubleType
district_clean_df = (district_df
    .select(
        F.col('A1').cast(IntegerType()).alias('district_id'),
        F.col('A2').cast(StringType()).alias('district_name'),
        F.col('A3').cast(StringType()).alias('region'),
        F.col('A4').cast(IntegerType()).alias('num_inhabitants'),
        F.col('A5').cast(IntegerType()).alias('municipalities_under_499'),
        F.col('A6').cast(IntegerType()).alias('municipalities_500_1999'),
        F.col('A7').cast(IntegerType()).alias('municipalities_2000_9999'),
        F.col('A8').cast(IntegerType()).alias('municipalities_over_10000'),
        F.col('A9').cast(IntegerType()).alias('num_cities'),
        F.col('A10').cast(DoubleType()).alias('urban_inhabitants_ratio'),
        F.col('A11').cast(DoubleType()).alias('avg_salary'),
        F.expr("try_cast(A12 as DOUBLE)").alias('unemployment_rate_1995'),
        F.expr("try_cast(A13 as DOUBLE)").alias('unemployment_rate_1996'),
        F.col('A14').cast(IntegerType()).alias('entrepreneurs_per_1000'),
        F.expr("try_cast(A15 as INT)").alias('crimes_1995'),
        F.expr("try_cast(A16 as INT)").alias('crimes_1996')
    )
)

# COMMAND ----------

display(district_clean_df)

# COMMAND ----------

district_dropped_df =( district_clean_df
                        .filter(F.col('district_id').isNotNull())
                        .dropDuplicates()
                        .withColumn('region', F.initcap(F.col('region')))
 
)

# COMMAND ----------

(
    district_dropped_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(f'{silver_schema}.districts')
)

# COMMAND ----------

