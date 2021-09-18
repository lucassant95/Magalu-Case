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
spark = SparkSession.builder.appName('[CURATED] Orders')         .master("spark://spark-master:7077")         .config("spark.sql.streaming.schemaInference", True)         .config("spark.sql.warehouse.dir", '/opt/workspace/Warehouse')         .enableHiveSupport()         .config("spark.sql.sources.partitionOverwriteMode", 'dynamic')         .getOrCreate()


# In[3]:


# This is an ugly query, I admit :(
# But in cases like the one presented here, there are two distinct products with the same total value for
# the same order. So even comparing the values, we can't determine the source partner that sold the product
# To continue this 
spark.sql(
    """
    with a as (
        select 
            id_pedido,
            count(*) as c
        from raw.pedido
        group by id_pedido
    ),
    b as (
        select id_pedido 
        from a 
        where c > 1
    ),
    c as (
    select 
        p.id_pedido || ip.id_produto || p.id_parceiro as order_unique_id,
        p.id_pedido,
        p.id_parceiro,
        p.id_cliente,
        p.id_filial,
        p.vr_total_pago,
        ip.id_produto,
        ip.quantidade,
        ip.vr_unitario,
        case when (vr_unitario * quantidade) = vr_total_pago then true end as real_item,
        to_date(p.dt_pedido) as partition
    from raw.pedido p
        join raw.item_pedido ip on ip.id_pedido = p.id_pedido
    where p.id_pedido in (select * from b) and p.id_pedido = 498735617
    )
    select * from c where real_item is not null
""").show()


# In[4]:


# This is an ugly query, I admit :(
# But in cases like the one presented here, there are two distinct products with the same total value for
# the same order. So even comparing the values, we can't determine the source partner that sold the product
# To continue this case, I'll be desconsidering orders with this behaviour! :D
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW untrackable_orders as
    with a as (
        select 
            id_pedido,
            count(*) as c
        from raw.pedido
        group by id_pedido
    ),
    b as (
        select id_pedido 
        from a 
        where c > 1
    ),
    c as (
    select 
       p.id_pedido,
       ip.id_produto,
       vr_unitario,
       quantidade,
       vr_total_pago,
       case when (vr_unitario * quantidade) = vr_total_pago then true end as related_item
    from raw.pedido p
        join raw.item_pedido ip on ip.id_pedido = p.id_pedido
    where p.id_pedido in (select * from b)
    ),
    d as (
    select 
        count(distinct (vr_unitario * quantidade)) as calculated_vr_total_pago,
        id_pedido,
        count(distinct id_produto) as total_dintinct_products
    from c 
    where related_item is not null
    group by id_pedido)
    select 
        id_pedido
    from d
    where calculated_vr_total_pago < total_dintinct_products
""")

spark.sql('select * from untrackable_orders').show()


# In[5]:


# This is an ugly query, I admit :(
# But in cases like the one presented here, there are two distinct products with the same total value for
# the same order. So even comparing the values, we can't determine the source partner that sold the product
# To continue this case, I'll be desconsidering orders with this behaviour! :D
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW normalized_orders as 
    with a as (
        select 
            id_pedido,
            count(*) as c
        from raw.pedido
        group by id_pedido
    ),
    b as (
        select id_pedido 
        from a 
        where c > 1
    ),
    c as (
    select 
       p.*,
       ip.id_produto,
       ip.quantidade,
       ip.vr_unitario,
       case when (vr_unitario * quantidade) = vr_total_pago then true end as related_item
    from raw.pedido p
        join raw.item_pedido ip on ip.id_pedido = p.id_pedido
    where 
        p.id_pedido in (select * from b)
        and p.id_pedido not in (select * from untrackable_orders)
    ),
    normalized_orders as (
    select 
        id_pedido,
        id_parceiro,
        id_cliente,
        id_filial,
        vr_total_pago,
        partition,
        id_produto,
        quantidade,
        vr_unitario
    from c
    where related_item = true)
    select
        p.id_pedido,
        id_parceiro,
        id_cliente,
        id_filial,
        vr_total_pago,
        partition,
        ip.id_produto,
        quantidade,
        vr_unitario
    from raw.pedido p
        join raw.item_pedido ip on ip.id_pedido = p.id_pedido
    union all
    select * from normalized_orders
        
""")


# In[6]:


df = spark.sql("""
    select 
        o.id_pedido || o.id_produto || o.id_parceiro as order_unique_id,
        o.id_pedido,
        o.id_parceiro,
        o.id_cliente,
        o.id_filial,
        f.id_cidade,
        e.id_estado,
        o.id_produto,
        c.id_categoria as categoria,
        sc.id_subcategoria as subcategoria,
        o.partition,
        collect_list(map('id_produto', o.id_produto,
                        'qtd', o.quantidade,
                        'vr_unitario', o.vr_unitario)) as order_partner_items,
        round(o.vr_total_pago) as order_partner_value
        from normalized_orders o
            join raw.produto p on p.id_produto = o.id_produto
            join raw.subcategoria sc on sc.id_subcategoria = p.id_subcategoria
            join raw.categoria c on c.id_categoria = sc.id_categoria
            join raw.filial f on f.id_filial = o.id_filial
            join raw.cidade ci on ci.id_cidade = f.id_cidade
            join raw.estado e on e.id_estado = ci.id_estado
        group by o.id_produto, o.id_pedido, o.id_parceiro, o.id_cliente, o.id_filial, f.id_cidade, e.id_estado, o.partition,
        c.id_categoria, sc.id_subcategoria, vr_total_pago
""")


# In[7]:


from magaluTools.dataValidation import *

validator = MagaluValidator().setDfToValidate(df).setValidation('order_unique_id', UniqueKeyValidator)
validator.validate(True)

df.write.partitionBy('partition').format('parquet').mode('overwrite').saveAsTable('curated.normalized_orders')


# In[8]:


# Stops spark client and finishes the job
spark.stop()

