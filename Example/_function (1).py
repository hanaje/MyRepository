# Databricks notebook source
import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from delta.tables import *
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

### MOUNT ADLS CONTAINER
def mount_adls(container):
    configs = {
        "fs.azure.account.auth.type": "OAuth"
        ,"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        ,"fs.azure.account.oauth2.client.id": "afcc1f29-732c-4c8b-a224-eb1eaee1a4d1"
        ,"fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "key-vault-secret", key = "KVS-FRW-APP")
        ,"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/3026e9ae-1c6f-48bc-aa40-711484d97639/oauth2/token"
    }
    dbutils.fs.mount(
        source = f"abfss://{container}@adlsadfdbtfdatabrickprod.dfs.core.windows.net/"
        ,mount_point = f"/mnt/{container}"
        ,extra_configs = configs
    )

# COMMAND ----------

### UNMOUNT ADLS CONTAINER
def unmount_adls(container):
    dbutils.fs.unmount(f"/mnt/{container}")

# COMMAND ----------

### LOAD PARQUET FROM ADLS  (Not mounted)
def adls_connect_src(containerName, directoryName):
    storageAccount = "adlsadfdbtfdatabrickprod"
    scopeName = "key-vault-secret"
    keyName = "KVS-FRW-ADLS"
    
    service_credential = dbutils.secrets.get(scope=scopeName,key=keyName)
    spark.conf.set(
        f'fs.azure.account.key.{storageAccount}.dfs.core.windows.net',
        service_credential
    )
    
    base_dir = f'abfs://{containerName}@{storageAccount}.dfs.core.windows.net'
    
    def list_directory_contents_inc():
        try:
            file_system_client = service_client.get_file_system_client(file_system=containerName)
            paths = file_system_client.get_paths(path=directoryName)
            last_file = sorted(paths, key=lambda path: path['last_modified'])[-1]
            return last_file.name
        except Exception as e:
            print(e)
            return
    
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storageAccount), credential=service_credential)
    ds_filepath = list_directory_contents_inc()
    fullpath = os.path.join(base_dir, ds_filepath)
    
    return spark.read.parquet(fullpath)
