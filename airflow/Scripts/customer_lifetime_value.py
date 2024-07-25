
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

dff_items = spark_con.read.csv("olist_order_items_dataset.csv",header=True,inferSchema=True)
dff_orders = spark_con.read.csv("olist_orders_dataset.csv",header=True,inferSchema=True)
dff_products = spark_con.read.csv('olist_products_dataset.csv',header=True,inferSchema=True)
dff_customers = spark_con.read.csv("olist_customers_dataset.csv", header=True, inferSchema=True)
dff_payments = spark_con.read.csv("olist_order_payments_dataset.csv", header=True, inferSchema=True)
dff_reviews = spark_con.read.csv("olist_order_reviews_dataset.csv", header=True, inferSchema=True)
dff_geolocation = spark_con.read.csv("olist_geolocation_dataset.csv", header=True, inferSchema=True)
dff_sellers = spark_con.read.csv("olist_sellers_dataset.csv", header=True, inferSchema=True)

dff_items.head()

# Create SQL Tables from dfs
dff_items.createOrReplaceTempView('items')
dff_orders.createOrReplaceTempView('orders')
dff_products.createOrReplaceTempView('products')
dff_customers.createOrReplaceTempView('customers')
dff_payments.createOrReplaceTempView('payments')
dff_reviews.createOrReplaceTempView('reviews')
dff_geolocation.createOrReplaceTempView('geolocation')
dff_sellers.createOrReplaceTempView('sellers')

"""**Customer Lifetime Value (CLV)**"""

# Join orders, payments, and customers to calculate CLV
customers_lv = spark_con.sql("""
SELECT
    c.customer_unique_id,
    SUM(p.payment_value) AS lifetime_value
FROM
    orders o
JOIN
    payments p ON o.order_id = p.order_id
JOIN
    customers c ON o.customer_id = c.customer_id
GROUP BY
    c.customer_unique_id
ORDER BY
    lifetime_value DESC
""")

customers_lv.show()

csv_export(customers_lv)
