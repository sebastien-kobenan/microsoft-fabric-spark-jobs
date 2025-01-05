import Constants
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Get constants

fileFormat = Constants.fileFormat

calendarSource = Constants.calendarSource
calendarTable = Constants.calendarTable

categorySource = Constants.categorySource
subcategorySource = Constants.subcategorySource
productSource = Constants.productSource
productTable = Constants.productTable

customerSource = Constants.customerSource
customerTable = Constants.customerTable

saleSource = Constants.saleSource
saleTable = Constants.saleTable

# Load of Calendar table

def load_calendar(spark_session):
    
    df = spark_session\
        .read\
        .format(fileFormat)\
        .option("header", "true")\
        .load(calendarSource)

    df = df.withColumn("Date", to_date("Date", "M/d/y"))

    df = df.withColumn("Year", year(df["Date"]))\
        .withColumn("MonthNumber", month(df["Date"]))\
        .withColumn("MonthName", date_format(df["Date"], "MMM"))\
        .withColumn("Quarter", date_format(df["Date"], "QQQ"))\
        .withColumn("DayOfWeek", date_format(df["Date"], "E"))\
        .withColumn("DayOfMonth", date_format(df["Date"], "d"))

    df = df.withColumn("DayOfMonth", col("DayOfMonth").cast(IntegerType()))

    spark_session.sql(f"DROP TABLE IF EXISTS {calendarTable}")

    df.write.mode("overwrite").format("delta").saveAsTable(calendarTable)


# Load of Products table

def load_products(spark_session):
    
    df_categories = spark_session\
        .read.format(fileFormat)\
        .option("header","true")\
        .load(categorySource)
    
    df_categories = df_categories\
        .withColumn("ProductCategoryKey", col("ProductCategoryKey").cast(IntegerType()))

    df_subcategories = spark_session\
        .read.format(fileFormat)\
        .option("header","true")\
        .load(subcategorySource)
    
    df_subcategories = df_subcategories\
        .withColumn("ProductSubcategoryKey", col("ProductSubcategoryKey").cast(IntegerType()))\
        .withColumn("ProductCategoryKey", col("ProductCategoryKey").cast(IntegerType()))

    df_products = spark_session\
        .read.format(fileFormat)\
        .option("header","true")\
        .load(productSource)

    df_products = df_products\
        .withColumn("ProductKey", col("ProductKey").cast(IntegerType()))\
        .withColumn("ProductSubcategoryKey", col("ProductSubcategoryKey").cast(IntegerType()))\
        .withColumn("ProductCost", col("ProductCost").cast(DecimalType(12, 2)))\
        .withColumn("ProductPrice", col("ProductPrice").cast(DecimalType(12, 2)))
    
    # Join categories, subcategories and products
    
    df_cat_sub_prod = df_categories\
        .join(df_subcategories, df_categories.ProductCategoryKey == df_subcategories.ProductCategoryKey, "inner")\
        .join(df_products, df_subcategories.ProductSubcategoryKey == df_products.ProductSubcategoryKey, "inner")

    # Select necessary columns

    df_cat_sub_prod = df_cat_sub_prod\
    .select(
        col("ProductKey"),
        col("CategoryName"),
        col("SubcategoryName"),
        col("ProductName"),   
        col("ProductSKU"),
        col("ModelName"),
        col("ProductDescription"),
        col("ProductColor"),
        col("ProductSize"),
        col("ProductStyle"),
        col("ProductCost"),
        col("ProductPrice")
    )

    spark_session.sql(f"DROP TABLE IF EXISTS {productTable}")

    df_cat_sub_prod.write.mode("overwrite").format("delta").saveAsTable(productTable)


# Load of Customers table

def load_customers(spark_session):
    
    df_customers = spark_session\
        .read\
        .format(fileFormat)\
        .option("header","true")\
        .load(customerSource)
    
    df_customers = df_customers\
    .withColumn("CustomerKey", col("CustomerKey").cast(IntegerType()))\
    .withColumn("BirthDate", to_date("BirthDate", "M/d/y"))\
    .withColumn("AnnualIncome", regexp_replace("AnnualIncome", "\$|,", "").cast(DecimalType(12,2)))\
    .withColumn("TotalChildren", col("TotalChildren").cast(IntegerType()))

    spark_session.sql(f"DROP TABLE IF EXISTS {customerTable}")

    df_customers.write.mode("overwrite").format("delta").saveAsTable(customerTable)


# Load of Sales table

def load_sales(spark_session):
    
    df_sales = spark_session\
        .read.format(fileFormat)\
        .option("header","true")\
        .load(saleSource)

    df_sales = df_sales\
        .withColumn("OrderDate", to_date("OrderDate", "M/d/y"))\
        .withColumn("StockDate", to_date("StockDate", "M/d/y"))\
        .withColumn("ProductKey", col("ProductKey").cast(IntegerType()))\
        .withColumn("CustomerKey", col("CustomerKey").cast(IntegerType()))\
        .withColumn("TerritoryKey", col("TerritoryKey").cast(IntegerType()))\
        .withColumn("OrderLineItem", col("OrderLineItem").cast(IntegerType()))\
        .withColumn("OrderQuantity", col("OrderQuantity").cast(IntegerType()))\
        .withColumn("UnitPrice", col("UnitPrice").cast(DecimalType(12, 2)))
    
    spark_session.sql(f"DROP TABLE IF EXISTS {saleTable}")

    df_sales.write.mode("overwrite").format("delta").saveAsTable(saleTable)

    