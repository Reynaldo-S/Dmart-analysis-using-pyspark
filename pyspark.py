# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 
	   
# Creating dataframes from the CSV files
customers_df = spark.read.csv("/FileStore/tables/Customer.csv", header=True, inferSchema=True)
products_df = spark.read.csv("/FileStore/tables/Product.csv", header=True, inferSchema=True)
sales_df = spark.read.csv("/FileStore/tables/Sales.csv", header=True, inferSchema=True)

# To Check the schema of the created dataframes from the csv files 
print('Schema_info and the first two rows for products_df')
products_df.printSchema()
products_df.show(2)

print('Schema_info and the first two rows for sales_df')
sales_df.printSchema()
sales_df.show(2)

print('Schema_info and the first two rows for customers_df')
customers_df.printSchema()
customers_df.show(2)

# Transformation - changing the column names
for col in products_df.columns:
    if " " or "-" in col:
        products_df = products_df.withColumnRenamed(col, col.replace(" ", "_").replace("-","_"))

products_df.show(2)

for col in sales_df.columns:
    if " " in col:
        sales_df = sales_df.withColumnRenamed(col, col.replace(" ", "_"))

sales_df.show(2)

for col in customers_df.columns:
    if " " in col:
        customers_df = customers_df.withColumnRenamed(col, col.replace(" ", "_"))

customers_df.show(2)


# Handling the missing values
from pyspark.sql.functions import col, sum

missing_values_products = products_df.select( [sum(col(column).isNull().cast("int")).alias(column) for column in products_df.columns] )

missing_values_sales = sales_df.select( [sum(col(column).isNull().cast("int")).alias(column) for column in sales_df.columns] )

missing_values_customers = customers_df.select( [sum(col(column).isNull().cast("int")).alias(column) for column in customers_df.columns] )

missing_values_products.show()

missing_values_sales.show()

missing_values_customers.show()

# Joining the dataframes
combined_df = sales_df.join(products_df, "Product_id").join(customers_df, "Customer_id")

# The total sales for each product category
total_sales_by_category = df.groupBy("Category").sum("Sales")
total_sales_by_category.show()

# customer has made the highest number of purchases
highest_purchasing_customer = df.groupBy("CustomerID").count().orderBy("count", ascending=False)
highest_purchasing_customer.show(1)

# The average discount given on sales across all products
average_discount = combined_df.select(format_number((avg("Discount")*100), 2).alias("Average_Discount"))
average_discount.show()

# Unique products sold in each region
unique_products_by_region = combined_df.groupBy("Region").agg(countDistinct("Product_ID").alias("Unique_Products_Sold")) 
unique_products_by_region.show()

# The total profit generated in each state
total_profit_by_state = combined_df.groupBy("State").sum("Profit") 
total_profit_by_state.show()

# Product in the sub-category has the highest sales
highest_sales_sub_category = combined_df.groupBy("Sub_Category").sum("Sales").orderBy("sum(Sales)", ascending=False)
highest_sales_sub_category.show(1)

# The average age of customers in each segment
average_age_by_segment = combined_df.groupBy("Segment").avg("Age")
average_age_by_segment.show()

# Orders shipped in each shipping mode
orders_by_shipping_mode = combined_df.groupBy("Ship_Mode").count()
orders_by_shipping_mode.show()

# The total quantity of products sold in each city
total_quantity_by_city = combined_df.groupBy("City").sum("Quantity")
total_quantity_by_city.show()

# customer segment has the highest profit margin
highest_profit_segment = combined_df.groupBy("Segment").sum("Profit").orderBy("sum(Profit)", ascending=False).show(1)


