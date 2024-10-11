# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from datetime import date
import io
from pyspark.sql.utils import AnalysisException


# COMMAND ----------

#ni to access whatever thats in my own blob
storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="aidaadls-key-no-peeking")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.aidaadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

today=date.today()

cleaned_output = f"abfss://try@aidaadls.dfs.core.windows.net/pekab40ALL{today}"
print(cleaned_output)

# COMMAND ----------

df = (spark.read
      .option('header', 'true')
      .option('inferSchema', 'true')
      .csv(cleaned_output)
      )

# COMMAND ----------

display(df)
