
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

"""**Customer Churn prediction**"""

last_purchase_date = spark_con.sql("SELECT MAX(order_purchase_timestamp) AS last_purchase_date FROM orders").collect()[0]['last_purchase_date']
churn_threshold_date = spark_con.sql(f"SELECT DATE_SUB('{last_purchase_date}', 180) AS churn_threshold_date").collect()[0]['churn_threshold_date']

# Calculate churn rate by region
Churn_rate_Prediction = spark_con.sql(f"""
WITH first_order AS (
    SELECT customer_id, MIN(order_purchase_timestamp) AS first_order_date
    FROM orders
    GROUP BY customer_id
),
last_order AS (
    SELECT customer_id, MAX(order_purchase_timestamp) AS last_order_date
    FROM orders
    GROUP BY customer_id
),
customer_lifetime AS (
    SELECT
        fo.customer_id,
        DATEDIFF(lo.last_order_date, fo.first_order_date) AS customer_lifetime_days,
        lo.last_order_date,
        g.geolocation_state AS state
    FROM
        first_order fo
    JOIN
        last_order lo ON fo.customer_id = lo.customer_id
    JOIN
        customers c ON fo.customer_id = c.customer_id
    JOIN
        geolocation g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
)
SELECT
    state,
    COUNT(customer_id) AS num_customers,
    (SUM(CASE WHEN last_order_date < '{churn_threshold_date}' THEN 1 ELSE 0 END) / COUNT(customer_id)) * 100 AS churn_rate
FROM
    customer_lifetime
GROUP BY
    state
ORDER BY
    churn_rate DESC
""")

Churn_rate_Prediction.show()

csv_export(Churn_rate_Prediction)
