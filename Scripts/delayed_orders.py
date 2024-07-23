# -*- coding: utf-8 -*-
"""delayed_orders.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/151VGRlo0rySLSdazynDg--ZzHaRMHjgD
"""

import pandas as pd
import boto3
from boto3.s3.transfer import S3Transfer
import os
from io import BytesIO
import findspark
findspark.init()
import shutil


from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import col, when, lit, count, sum, avg, max, datediff, current_date
from pyspark.sql.functions import *

#  Spark configuration and context
spark_con = SparkSession.builder.appName("Olist-ETL").getOrCreate()
from pyspark.sql import SQLContext
sqlContext = SQLContext(spark_con)
spark_con.conf.set("spark.sql.execution.arrow.enabled", "true")

def get_dfname(df):
    name =[xi for xi in globals() if globals()[xi] is df][0]
    return name

def csv_export(dataframe: DataFrame):
    df_name = get_dfname(dataframe)
    output_dir = "/content/kpi"

    temp_dir = f"{output_dir}/temp_{df_name}"
    output_file = f"{output_dir}/{df_name}.csv"

    # Coalesce to single partition
    dataframe.coalesce(1).write.option("header", "true").csv(temp_dir)

    temp_file = [file for file in os.listdir(temp_dir) if file.startswith("part-")][0]
    shutil.move(f"{temp_dir}/{temp_file}", output_file)

    # Remove the temporary directory
    shutil.rmtree(temp_dir)
    print(f"CSV file created at: {output_file}")

"""**delayed_orders.csv**"""

delayed_orders = spark_con.sql("""
SELECT
    i.order_id,
    i.seller_id,
    i.shipping_limit_date,
    i.price,
    i.freight_value,
    p.product_id,
    p.product_category_name,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date
FROM
    items AS i
JOIN
    orders AS o
ON
    i.order_id = o.order_id
JOIN
    products AS p
ON
    i.product_id = p.product_id
WHERE
    i.shipping_limit_date < o.order_delivered_carrier_date
""")

delayed_orders.show()

csv_export(delayed_orders)