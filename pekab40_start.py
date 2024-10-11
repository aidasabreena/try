# Databricks notebook source
# MAGIC %pip install tqdm
# MAGIC %pip install bs4
# MAGIC %pip install playwright
# MAGIC %pip install lxml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import re
import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import date

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="accesskey-adls-adlskotaksakti1")

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="aidaadls-key-no-peeking")

# COMMAND ----------

url = "https://bms.pekab40.com.my/provider/index?page={}&per-page=10"
all_rows = []
for i in range(1,469):
    response = requests.get(url.format(i))
    soup = BeautifulSoup(response.content, "lxml")
    rows = soup.find("table", class_="table").find_all("tr")[2:]
    all_rows.extend(rows)
    print(f"Page {i}: {len(rows)} rows found")
#scrape data from all pages, and then for every rows found in every page, it will merge all to all_rows


# COMMAND ----------

data = []
for row in all_rows:
    row_data = [cell.text.strip() for cell in row.find_all("td")]
    data.append(row_data)
print(len(data))
# finds all td elements from the pages, and then for every td element, it will find the text and then strip the whitespace, and put the elements to a list. This will collect and clean the test data from each cell in every row found across all pages.

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

today= date.today()

# COMMAND ----------

output_folder_path = f"abfss://try@aidaadls.dfs.core.windows.net/pekab40ALL{today}"

# COMMAND ----------

df.write.format("csv").option("inferSchema", "true").option("header", "true").save(output_folder_path)
#df.write.mode("overwrite").parquet(output_folder_path)

# COMMAND ----------

display(df)
