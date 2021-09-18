#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os

# Changing working directory to root to use custom libs
os.chdir('/opt/workspace/')

# Libs import
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import date_format, col, to_date, isnan
import pyspark
import sys
from pyspark.sql.utils import AnalysisException
from pathlib import Path
sys.path.append('/opt/workspace/')
import yaml
from magaluTools.logger import get_logger
from magaluTools.dataValidation import *
from magaluTools.etl import src2raw


# In[2]:


#Spark Configurations
    # Sets Session to use spark master container
    # Sets Session to use warehouse directory in /opt/workspace/Warehouse and to infer data Schema
    # Sets overide mode to dynamic, so we can append data and overwrite old data based on partition
spark = SparkSession.builder.appName('warehouse_setup')         .master("spark://spark-master:7077")         .config("spark.sql.streaming.schemaInference", True)         .config("spark.sql.warehouse.dir", '/opt/workspace/Warehouse')         .enableHiveSupport()         .config("spark.sql.sources.partitionOverwriteMode", 'dynamic').getOrCreate()


# In[3]:


# Creating warehouse databases

spark.sql('create database if not exists raw')
spark.sql('create database if not exists curated')


# In[4]:


# Getting the metadata for correctly dumping the csv files to the warehouse

with open('./dump_metadata.yml', 'r') as f:
            dump_metadata = yaml.load(f)


# In[5]:


# Iterating over the files in /Data/ Folder to dump the data to the Warehouse

for x in Path('./Data').iterdir():
    
    # Checking if the the current entity is a file or directory
    if not x.is_dir(): 
        
        # Seting the name of the table
        table_name = x.name.replace('.csv','')
        
        # Checking if the metadata needed for the etl is in the file, if not, skips the file
        if table_name in dump_metadata:
            
            # Getting the metadata specicallly for the current dump
            metadata = dump_metadata[table_name]
            
            # Using custom function to extract the file data and transform it into parquet files in the warehouse
            result_df = src2raw(file_path=f'/opt/workspace/Data/{x.name}',
                                table_name=table_name,
                                validations=metadata.get('validations'),
                                partition_data=metadata.get('partition'),
                                csv_delimiter=metadata.get('delimiter'),
                                raise_if_validation_failed=True)
    
        # Printing warning for the user to know the metadata for the csv file was not found
        else:
            get_logger().warning(f'CSV metadata not found for {x.name}, please create it in the dump_metadata.yml file :D')


# In[6]:


# Stops Spark and finishes the job
spark.stop()

