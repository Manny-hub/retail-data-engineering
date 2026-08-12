import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, desc, count

def main():
    # 1. Initialize Spark
    spark = SparkSession.builder \
        .appName("RetailFlow_Analytics") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    gold_dir = "./data/gold"

    # 2. Load Gold Tables
    print("Loading Gold tables for analytics...")
    fact_sales = spark.read.parquet(os.path.join(gold_dir, "fact_sales"))
    dim_product = spark.read.parquet(os.path.join(gold_dir, "dim_product"))
    dim_date = spark.read.parquet(os.path.join(gold_dir, "dim_date"))

    # ==========================================
    # 3. Executive KPIs
    # ==========================================
    print("\n" + "="*40)
    print("EXECUTIVE DASHBOARD SUMMARY")
    print("="*40)
    
    total_revenue = fact_sales.agg({"Revenue": "sum"}).first()[0]
    total_profit = fact_sales.agg({"Profit": "sum"}).first()[0]
    total_orders = fact_sales.select("OrderID_BK").distinct().count()

    print(f"Total Revenue:  £{total_revenue:,.2f}")
    print(f"Total Profit:   £{total_profit:,.2f}")
    print(f"Total Orders:   {total_orders:,}")
    print("="*40)

    # ==========================================
    # 4. Top 5 Products by Revenue
    # ==========================================
    print("\nTop 5 Products by Revenue:")
    top_products = fact_sales.join(dim_product, "ProductKey") \
        .groupBy("Category", "Brand") \
        .agg(round(sum("Revenue"), 2).alias("TotalRevenue")) \
        .orderBy(desc("TotalRevenue")) \
        .limit(5)
    
    top_products.show(truncate=False)

    # ==========================================
    # 5. Monthly Revenue Trend
    # ==========================================
    print("Monthly Revenue Trend (First 10 Months):")
    monthly_trend = fact_sales.join(dim_date, "DateKey") \
        .groupBy("Year", "Month") \
        .agg(round(sum("Revenue"), 2).alias("MonthlyRevenue")) \
        .orderBy("Year", "Month") \
        .limit(10)
    
    monthly_trend.show(truncate=False)

    print("Analytics complete!")
    spark.stop()

if __name__ == "__main__":
    main()