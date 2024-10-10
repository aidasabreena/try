# Databricks notebook source
#%pip install tqdm
#%pip install bs4
#%pip install playwright
#%pip install lxml

import re
import time
import random
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from tqdm import tqdm
import pandas as pd
import requests
from bs4 import BeautifulSoup

# COMMAND ----------

#
storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="accesskey-adls-adlskotaksakti1")

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="aidaadls-key-no-peeking")

# COMMAND ----------

#dbutils.library.restartPython()

# COMMAND ----------

url = "https://bms.pekab40.com.my/provider/index?page={}&per-page=10"  

for i in range(1,469):
 response = requests.get(url)
 soup = BeautifulSoup(response.content, "lxml")
#    Scrape data from the page
# url = "https://bms.pekab40.com.my/provider/index?page={}&per-page=10" 
# https://bms.pekab40.com.my/provider/index?page={}&per-page=10
# https://open.dosm.gov.my/data-catalogue/births_annual?visual=table
rows = soup.find("table", class_="table").find_all("tr")[2:]
print(rows)

# COMMAND ----------

data = []
for row in rows:
    row_data = [cell.text for cell in row.find_all("td")]
    data.append(row_data)
print(data)

#data = [p.table for p in soup.find_all("p")]

# COMMAND ----------

spark.conf.set("fs.azure.account.key.aidaadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

#df = pd.DataFrame(data, columns=["text"])
df = spark.createDataFrame(pd.DataFrame(data, 
            columns=[
            "#",
            "Clinic Name",
            "Address",
            "Postcode",
            "City",
            "State",
            "Contact number",
            "Is Public",
            "Location",
        ]))
#df.write.format("csv").option("inferSchema", "false").option("header", "true").save("abfss://try@aidaadls.dfs.core.windows.net/try1/pekab40.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

output_folder_path = f"abfss://try@aidaadls.dfs.core.windows.net/try/pekab40ALL.csv"

# COMMAND ----------

df.write.mode("overwrite").parquet(output_folder_path)
