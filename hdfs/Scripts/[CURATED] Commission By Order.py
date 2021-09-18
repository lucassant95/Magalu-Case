#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Libs import
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark
from pyspark.sql.utils import AnalysisException
import os
import sys
from pathlib import Path
sys.path.append('/opt/workspace/')


# In[2]:


# Changing working directory to root to use custom libs
os.chdir('/opt/workspace/')

#Spark Configurations
    # Sets Session to use spark master container
    # Sets Session to use warehouse directory in /opt/workspace/Warehouse and to infer data Schema
    # Sets overide mode to dynamic, so we can append data and overwrite old data based on partition
spark = SparkSession.builder.appName('[CURATED] Commission By Order')         .master("spark://spark-master:7077")         .config("spark.sql.streaming.schemaInference", True)         .config("spark.sql.warehouse.dir", '/opt/workspace/Warehouse')         .enableHiveSupport()         .config("spark.sql.sources.partitionOverwriteMode", 'dynamic')         .getOrCreate()


# In[5]:


df = spark.sql(
    """
    select 
        o.id_pedido,
        o.order_partner_value * 6 * 0.01 as order_commission,
        o.id_parceiro,
        o.partition
    from curated.normalized_orders o
        join raw.categoria c on c.id_categoria = o.categoria
""")

df.write.partitionBy('partition').format('parquet').mode('overwrite').saveAsTable('curated.orders_commission')


# In[6]:


spark.sql('select * from curated.orders_commission').show()


# In[7]:


# Stops spark client and finishes the job
spark.stop()

