from datetime import timedelta, datetime
from airflow import models
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow import DAG

###############################################
# Parameters
###############################################
spark_master = "spark://spark-master:7077"
spark_app_name = "Partner Commission Pipeline"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "pipeline",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["lucas.santanna@magalu.com.br"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

startup_dag = DAG(
    "environment_startup",
    default_args=default_args,
    schedule_interval=None
)

environment_startup = SparkSubmitOperator(
    task_id="curated_normalized_orders",
    application="Scripts/Warehouse Environment Initial Setup.py", # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={
        "spark.local.dir": '/opt/workspace/',
        "spark.sql.warehouse.dir": '/opt/workspace/Warehouse'
        },
    dag=startup_dag
)

dag = DAG(
        "commission-pipeline",
        default_args=default_args, 
        schedule_interval="0 0 1 * *"
     )

CURATED_Orders = SparkSubmitOperator(
    task_id="curated_normalized_orders",
    application="/opt/workspace/Scripts/[CURATED] Orders.py", # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={
        "spark.local.dir": '/opt/workspace/',
        "spark.sql.warehouse.dir": '/opt/workspace/Warehouse'
        },
    dag=dag)


CURATED_commission_by_order = SparkSubmitOperator(
    task_id="curated_commission_by_order",
    application="/opt/workspace/Scripts/[CURATED] Commission By Order.py",
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={
        "spark.local.dir": '/opt/workspace/',
        "spark.sql.warehouse.dir": '/opt/workspace/Warehouse'
        },
    dag=dag)

CURATED_commission_by_partner = SparkSubmitOperator(
    task_id="curated_commission_by_partner",
    application="/opt/workspace/Scripts/[CURATED] Commission By Partner.py",
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={
        "spark.local.dir": '/opt/workspace/',
        "spark.sql.warehouse.dir": '/opt/workspace/Warehouse'
        },
    dag=dag)

CURATED_Orders >> CURATED_commission_by_order >> CURATED_commission_by_partner
