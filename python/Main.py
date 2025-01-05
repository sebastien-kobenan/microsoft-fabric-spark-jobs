import Utils
from pyspark.sql import SparkSession

if __name__ == "__main__":

    #Spark session builder
    spark_session = (SparkSession
          .builder
          .appName("SparkJobSession")
          .getOrCreate())
    
    spark_context = spark_session.sparkContext
    spark_context.setLogLevel("DEBUG")

    Utils.load_calendar(spark_session)

    Utils.load_products(spark_session)

    Utils.load_customers(spark_session)

    Utils.load_sales(spark_session)

