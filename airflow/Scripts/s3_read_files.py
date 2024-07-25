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

from pyspark.sql import SparkSession

# Enter your AWS credentials
spark_con = SparkSession.builder \
    .appName("Ecommerce Analysis") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

dff_items = spark_con.read.csv("s3a://ecommercelogistics/unzip/olist_order_items_dataset.csv",header=True,inferSchema=True)
dff_orders = spark_con.read.csv("s3a://ecommercelogistics/unzip/olist_orders_dataset.csv",header=True,inferSchema=True)
dff_products = spark_con.read.csv("s3a://ecommercelogistics/unzip/olist_products_dataset.csv",header=True,inferSchema=True)
dff_customers = spark_con.read.csv("s3a://ecommercelogistics/unzip/olist_customers_dataset.csv", header=True, inferSchema=True)
dff_payments = spark_con.read.csv("s3a://ecommercelogistics/unzip/olist_order_payments_dataset.csv", header=True, inferSchema=True)
dff_reviews = spark_con.read.csv("s3a://ecommercelogistics/unzip/olist_order_reviews_dataset.csv", header=True, inferSchema=True)
dff_geolocation = spark_con.read.csv("s3a://ecommercelogistics/unzip/olist_geolocation_dataset.csv", header=True, inferSchema=True)
dff_sellers = spark_con.read.csv("s3a://ecommercelogistics/unzip/olist_sellers_dataset.csv", header=True, inferSchema=True)

# SQL Tables from dataframes
dff_items.createOrReplaceTempView('items')
dff_orders.createOrReplaceTempView('orders')
dff_products.createOrReplaceTempView('products')
dff_customers.createOrReplaceTempView('customers')
dff_payments.createOrReplaceTempView('payments')
dff_reviews.createOrReplaceTempView('reviews')
dff_geolocation.createOrReplaceTempView('geolocation')
dff_sellers.createOrReplaceTempView('sellers')

# Download all files as zip
!kaggle datasets download -d olistbr/brazilian-ecommerce
# Unzipping the downloaded dataset
!unzip brazilian-ecommerce.zip

spark_con.sql('SELECT * FROM items').columns

spark_con.sql('SELECT * FROM orders').columns

spark_con.sql('SELECT * FROM products').columns
