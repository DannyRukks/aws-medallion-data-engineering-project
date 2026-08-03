import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# Initialize Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Read Silver Tables
sales = glueContext.create_dynamic_frame.from_catalog(
    database="silver_db",
    table_name="sales").toDF()

customers = glueContext.create_dynamic_frame.from_catalog(
    database="silver_db",
    table_name="customers").toDF()

products = glueContext.create_dynamic_frame.from_catalog(
    database="silver_db",
    table_name="products").toDF()

stores = glueContext.create_dynamic_frame.from_catalog(
    database="silver_db",
    table_name="stores").toDF()

# CREATE DIMENSION TABLES
# Customer Dimension
dim_customers = (
    customers
    .select(
        "customerid",
        "gender",
        "city",
        "country",
        "region"
    )
)

# Product Dimension
dim_products = (
    products
    .select(
        "productid",
        "productname",
        "category",
        "costprice",
        "sellingprice"
   
    )
)
# Store Dimension
dim_stores = (
    stores
    .select(
        "storeid",
        "storename",
        "city",
        "country",
        "region"
    )
)
# CREATE FACT TABLE
fact_sales = (
    sales
    .join(products.select("productid", "sellingprice", "costprice"),
        on="productid",
        how="left"
    )
    .withColumn("totalsales", F.col("quantity") * F.col("sellingprice"))
    .withColumn("totalcost", F.col("quantity") * F.col("costprice"))
    .withColumn("totalprofit", F.col("totalsales") - F.col("totalcost"))
    .select(
        "saleid",
        "saledate",
        "year",
        "month",
        "customerid",
        "productid",
        "storeid",
        "quantity",
        "unitprice",
        "totalsales",
        "totalcost",
        "totalprofit"
    )
)
# Incremental Load using LEFT ANTI JOIN
def incremental_load(df, key_column, target_path, table_name):
    try:
        # Read existing target data
        existing_df = spark.read.parquet(target_path)

        # Keep only new records
        df = df.join(
            existing_df.select(key_column),
            on=key_column,
            how="left_anti"
        )
    except AnalysisException:
        print(f"{table_name}: First load - target folder does not exist.")

    return df

dim_customers = incremental_load(dim_customers,"customerid","s3://rukky-business-lake/data/gold/dim_customers/", "dim_customers")
dim_products = incremental_load(dim_products, "productid", "s3://rukky-business-lake/data/gold/dim_products/", "dim_products")
dim_stores = incremental_load(dim_stores, "storeid", "s3://rukky-business-lake/data/gold/dim_stores/", "dim_stores")
fact_sales = incremental_load(fact_sales, "saleid", "s3://rukky-business-lake/data/gold/fact_sales/",
 "fact_sales")
# WRITE DIMENSION TABLES
(
    dim_customers.write
    .mode("append")
    .parquet("s3://rukky-business-lake/data/gold/dim_customers/")
)

(
    dim_products.write
    .mode("append")
    .parquet("s3://rukky-business-lake/data/gold/dim_products/")
)

(
    dim_stores.write
    .mode("append")
    .parquet("s3://rukky-business-lake/data/gold/dim_stores/")
)

# WRITE FACT TABLE
(
    fact_sales.write
    .mode("append")
    .partitionBy("year", "month")
    .parquet("s3://rukky-business-lake/data/gold/fact_sales/")
)
job.commit()
