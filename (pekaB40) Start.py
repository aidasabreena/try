# Databricks notebook source
#%pip install tqdm
#%pip install bs4
#%pip install playwright

import re
import time
import random
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from tqdm import tqdm
import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# COMMAND ----------

#
storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="accesskey-adls-adlskotaksakti1")

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="aidaadls-key-no-peeking")

# COMMAND ----------

# MAGIC %pip install lxml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

url = "https://bms.pekab40.com.my/provider/index?page={}&per-page=10"  
# Replace with the actual URL you want to scrape
# https://bms.pekab40.com.my/provider/index?page={}&per-page=10
# https://open.dosm.gov.my/data-catalogue/births_annual?visual=table
response = requests.get(url)
soup = BeautifulSoup(response.content, "lxml")
rows = soup.find("table", class_="table").find_all("tr")[2:]

# COMMAND ----------

cols = rows[i].find_all("td")
location = cols[8].find("a")["href"] if cols[8].find("a") else ""
latitude, longitude = get_lat_long(location) if location else ("", "")

# COMMAND ----------

for attempt in range(MAX_RETRIES):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(location)

                try:
                    page.wait_for_url(PATTERN, timeout=60000)
                except (TimeoutError, PlaywrightTimeoutError):
                    print(
                        f"Timeout occurred on attempt {attempt + 1} for URL: {location}"
                    )
                    if attempt == MAX_RETRIES - 1:
                        return "", ""
                    continue

                time.sleep(random.uniform(0.5, 1.5))
                match = PATTERN.search(page.url)
                if match:
                    # return match.groups()
                    return tuple(str(g) for g in match.groups())

                print(
                    f"Latitude and longitude not found in the URL on attempt {attempt + 1}"
                )

        except Exception as e:
            print(f"An error occurred on attempt {attempt + 1}: {str(e)}")

        time.sleep(random.uniform(2, 5))

    print(f"All {MAX_RETRIES} attempts failed for URL: {location}")

# COMMAND ----------

data = [p.content for p in soup.find_all("p")]

# COMMAND ----------

spark.conf.set("fs.azure.account.key.aidaadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# df = pd.DataFrame(data, columns=["text"])
df = spark.createDataFrame(pd.DataFrame(data, columns=["content"]))
df.write.format("csv").option("inferSchema", "false").option("header", "true").save("abfss://try@aidaadls.dfs.core.windows.net/try1/pekab40.csv")

# COMMAND ----------

spark_df.show()

# COMMAND ----------

output_folder_path = f"abfss://try@aidaadls.dfs.core.windows.net/birth{today}"
