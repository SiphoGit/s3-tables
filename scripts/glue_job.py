import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

# Configure Spark session for Iceberg
spark_conf = SparkSession.builder.appName("GlueJob") \
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.warehouse", "PASTE YOUR TABLE BUCKET ARN HERE!") \
    .config("spark.sql.defaultCatalog", "s3tablesbucket") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.s3tablesbucket.cache-enabled", "false") \

# Initialize Glue context with custom Spark configuration
sc = SparkContext.getOrCreate(conf=spark_conf.getOrCreate().sparkContext.getConf())
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

name_space = "supermarket_data"
table_name = "sales"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name_space}")

query = f"""
    CREATE TABLE IF NOT EXISTS {name_space}.{table_name} (
        invoice_id STRING,
        branch STRING,
        city STRING,
        customer_type STRING,
        gender STRING,
        product_line STRING,
        unit_price DOUBLE,
        quantity INT,
        tax_5_percent DOUBLE,
        total DOUBLE,
        date STRING,
        time STRING,
        payment STRING,
        cogs DOUBLE,
        gross_margin_percentage DOUBLE,
        gross_income DOUBLE,
        rating DOUBLE
    ) USING ICEBERG
    PARTITIONED BY (branch)
"""
spark.sql(query)

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3://INSERT THE NAME OF YOUR GENERAL-PURPOSE BUCKET HERE/supermarket_sales .csv") \
    .withColumnRenamed("Invoice ID", "invoice_id") \
    .withColumnRenamed("Branch", "branch") \
    .withColumnRenamed("City", "city") \
    .withColumnRenamed("Customer type", "customer_type") \
    .withColumnRenamed("Gender", "gender") \
    .withColumnRenamed("Product line", "product_line") \
    .withColumnRenamed("Unit price", "unit_price") \
    .withColumnRenamed("Quantity", "quantity") \
    .withColumnRenamed("Tax 5%", "tax_5_percent") \
    .withColumnRenamed("Total", "total") \
    .withColumnRenamed("Date", "date") \
    .withColumnRenamed("Time", "time") \
    .withColumnRenamed("Payment", "payment") \
    .withColumnRenamed("cogs", "cogs") \
    .withColumnRenamed("gross margin percentage", "gross_margin_percentage") \
    .withColumnRenamed("gross income", "gross_income") \
    .withColumnRenamed("Rating", "rating")

# Convert the 'date' column from string to date type
df = df.withColumn("date", to_date(col("date"), "M/d/yyyy"))

# Write to Iceberg table
df.write.format("iceberg") \
    .mode("append") \
    .saveAsTable(f"{name_space}.{table_name}")

print("SELECT FROM TABLE:") 
get_data_query = spark.sql(f"SELECT * FROM {name_space}.{table_name} LIMIT 5").show()
print(get_data_query)   

job.commit()