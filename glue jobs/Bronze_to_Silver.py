import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the bronze tables
sales_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="bronze_db",
    table_name="sales"
)

customers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="bronze_db",
    table_name="customers"
)

products_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="bronze_db",
    table_name="products"
)

stores_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="bronze_db",
    table_name="stores"
)

# Convert to Spark DataFrames
sales = sales_dyf.toDF()
customers = customers_dyf.toDF()
products = products_dyf.toDF()
stores = stores_dyf.toDF()
sales.printSchema()

# Transformations on the sales dataframe
sales = (
    sales
    # Trim business keys
    .withColumn("SaleID", F.trim("SaleID"))
    .withColumn("CustomerID", F.trim("CustomerID"))
    .withColumn("ProductID", F.trim("ProductID"))
    .withColumn("StoreID", F.trim("StoreID"))
    

    # Convert date
    .withColumn("SaleDate", F.to_date("SaleDate", "yyyy-MM-dd"))

    # Remove duplicate transactions
    .dropDuplicates(["SaleID"])

    # Keep only valid Sale IDs
    .filter(
        F.col("SaleID").isNotNull() &
        (F.col("SaleID") != "")
    )

    # Keep only valid foreign keys
    .filter(
        F.col("CustomerID").isNotNull() &
        (F.col("CustomerID") != "") &
        F.col("ProductID").isNotNull() &
        (F.col("ProductID") != "") &
        F.col("StoreID").isNotNull() &
        (F.col("StoreID") != "")
    )

    # Remove invalid dates
    .filter(F.col("SaleDate").isNotNull())

    # Business validations
    .filter(F.col("Quantity") > 0)
    .filter(F.col("UnitPrice") > 0)

    # Partition columns
    .withColumn("year", F.year("SaleDate"))
    .withColumn("month", F.month("SaleDate"))
)

print("Sales count:", sales.count())
sales.printSchema()

# Transformations on the customer dataframe

# Trim CustomerID
customers = customers.withColumn("CustomerID", F.trim(F.col("CustomerID")))

# Trim relevant columns
columns = ["Gender", "City", "Country", "Region"]
for column in columns:
    customers = customers.withColumn(column, F.trim(F.col(column)))

# Replace missing and null values with unknown
for column in columns:
    customers = (
        customers
        .withColumn(
        column,
        F.when(F.col(column).isNull() | (F.col(column) == ""), "Unknown")
        .otherwise(F.col(column))
    )
)

# Remove duplicates from customer id field
customers = (
    customers
    .dropDuplicates(["CustomerID"])
    .filter(
        F.col("CustomerID").isNotNull() &
        (F.col("CustomerID") != ""))
)

# Transformations on the product dataframe

# Drop duplicate id and remove null rows and trim
# Clean ProductID
products = (
    products
    .withColumn("ProductID", F.trim(F.col("ProductID")))
    .dropDuplicates(["ProductID"])
    .filter(
        F.col("ProductID").isNotNull() &
        (F.col("ProductID") != "")
    )
)

# Trim descriptive columns
columns = ["ProductName", "Category"]
for column in columns:
    products = products.withColumn(
        column,
        F.trim(F.col(column))
    )

# Replace missing descriptive values
for column in columns:
    products = products.withColumn(
        column,
        F.when(
            F.col(column).isNull() | (F.col(column) == ""),
            "Unknown"
        ).otherwise(F.col(column))
    )

# Business validation
products = (
    products
    .filter(F.col("CostPrice") > 0)
    .filter(F.col("SellingPrice") > 0)
)

# Standardize category names
products = products.withColumn(
    "Category",
    F.initcap(F.col("Category"))
)

# Transformations on the store dataframe
stores = (
    stores
    .withColumn("StoreID", F.trim(F.col("StoreID")))
    .dropDuplicates(["StoreID"])
    .filter(F.col("StoreID").isNotNull() & (F.col("StoreID") != ''))
)

# Trim relevant columns
columns = ["StoreName", "City", "Country", "Region"]
for column in columns:
    stores = stores.withColumn(
        column,
        F.trim(F.col(column))
    )

# replace the missing values with unknown
for column in columns:
    stores = stores.withColumn(
        column,
        F.when(
            F.col(column).isNull() | (F.col(column) == ''),
            "Unknown"
            ).otherwise(F.col(column))
        )

# Standardize category names
columns = ["City", "Country", "Region"]
for column in columns:
    stores = stores.withColumn(
        column,
        F.initcap(F.col(column))
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

sales = incremental_load(sales, "SaleID", "s3://rukky-business-lake/data/silver/sales/",
 "sales")
customers = incremental_load(customers,"CustomerID","s3://rukky-business-
lake/data/silver/customers","customers")
products = incremental_load(products, "ProductID", "s3://rukky-business-
lake/data/silver/products/", "products")
stores = incremental_load(stores, "StoreID", "s3://rukky-business-lake/data/silver/stores/", "stores")

# Write Sales
(
    sales.write
    .mode("append")
    .partitionBy("year", "month")
    .parquet("s3://rukky-business-lake/data/silver/sales/")
)

# Write Products
(
    products.write
    .mode("append")
    .parquet("s3://rukky-business-lake/data/silver/products/")
)

# Write Customers
(
    customers.write
    .mode("append")
    .parquet("s3://rukky-business-lake/data/silver/customers/")
)

# Write Stores
(
    stores.write
    .mode("append")
    .parquet("s3://rukky-business-lake/data/silver/stores/")
)
job.commit()

