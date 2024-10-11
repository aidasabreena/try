# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from datetime import date
import io

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
#read data from dataset dalam blob

# COMMAND ----------

df.show()

# COMMAND ----------

#  first calculates the summary statistics for the DataFrame df and then converts the resulting PySpark DataFrame into a Pandas DataFrame.
df.describe().toPandas()

# COMMAND ----------

#shows the count of peka clinics from each state in the DataFrame. 
df_state = df.groupBy("State").count()
df_state.show()

# COMMAND ----------

today= date.today()
output_folder_path = f"abfss://try@aidaadls.dfs.core.windows.net/pekab40bystate{today}"

# COMMAND ----------

# Write the DataFrame to a Parquet file
df_state.write.mode("overwrite").parquet(output_folder_path)
#df_state.write.format("csv").option("inferSchema", "true").option("header", "true").save(output_folder_path)

# COMMAND ----------

import matplotlib.pyplot as plt

# Create a sample DataFrame
df = df_state.toPandas()
# Create a line plot
plt.plot(df['State'], df['count'])
plt.xlabel('State')
plt.ylabel('Count')
plt.title('PekaB40 Clinics By State')

# Display the plot in Databricks
display(plt)
