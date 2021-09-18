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


# Changing working directory to root to use custom libraries
os.chdir('/opt/workspace/')

#Spark Configurations
    # Sets Session to use spark master container
    # Sets Session to use warehouse directory in /opt/workspace/Warehouse and to infer data Schema
    # Sets overide mode to dynamic, so we can append data and overwrite old data based on partition
spark = SparkSession.builder.appName('[Curated] Commission By Partner')         .master("spark://spark-master:7077")         .config("spark.sql.streaming.schemaInference", True)         .config("spark.sql.warehouse.dir", '/opt/workspace/')         .enableHiveSupport()         .config("spark.sql.sources.partitionOverwriteMode", 'dynamic')         .getOrCreate()


# In[5]:


df = spark.sql(
    """
    with base as (
    select 
        o.id_parceiro,
        date_format(o.partition, 'y-M') as commission_month_reference,
        round(sum(o.order_commission),2) as partner_commission_without_bonus,
        floor(sum(o.order_commission)/10000) * 100 as partner_bonus
    from curated.orders_commission o
    group by o.id_parceiro, o.partition
    )
    select *, partner_commission_without_bonus - partner_bonus as partner_commission_with_bonus  from base
""")

df.write.partitionBy('commission_month_reference').format('parquet').mode('overwrite').saveAsTable('curated.partners_commissions_by_month')


# In[6]:


spark.sql('select * from curated.partners_commissions_by_month').show()


# In[7]:


# Stops spark client and finishes the job
spark.stop()

