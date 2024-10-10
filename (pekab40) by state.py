# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from datetime import date
import io

# COMMAND ----------

#ni kalau ambil campaign_csv
storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="accesskey-adls-adlskotaksakti1")

# COMMAND ----------

#ni to access whatever thats in my own blob
storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="aidaadls-key-no-peeking")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.aidaadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv("abfss://try@aidaadls.dfs.core.windows.net/pekaB40/PEKAb40_clinic_data_test.csv")
      )
#read data from dataset dalam blob

# COMMAND ----------

df.show(1)

# COMMAND ----------

#  first calculates the summary statistics for the DataFrame df and then converts the resulting PySpark DataFrame into a Pandas DataFrame.
df.describe().toPandas()

# COMMAND ----------

#shows the count of peka clinics from each state in the DataFrame. 
df.groupBy("State").count().show()

# COMMAND ----------

# df.select(countDistinct("Marital_Status")).show() creates a new DataFrame containing only one column. This column holds the count of distinct values in the "Marital_Status" column of the original DataFrame df.

# Write the DataFrame to a Parquet file
# df.write.mode("overwrite").parquet(output_folder_path)
