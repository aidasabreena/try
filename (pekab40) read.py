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
      .option("delimiter", "|")
      .csv("abfss://try@aidaadls.dfs.core.windows.net/try/pekab40ALL.csv")
#      .csv("abfss://try@aidaadls.dfs.core.windows.net/pekaB40/PEKAb40_clinic_data_test.csv")
      )
#read data from dataset dalam blob

# COMMAND ----------

df.show(10)
