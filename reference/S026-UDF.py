# Databricks notebook source
# UDF - User Defined Functions
# UDF - User Defined Functions, custom functions written in scala,java, python 
# usedin spark sql, spark dataframe
# python - slow
# scala, java - fast

# COMMAND ----------

# Databricks notebook source
# UDF - User Defined Functions
# useful to extend spark sql functions with custom code

power = lambda n : n * n

from pyspark.sql.functions import udf 
from pyspark.sql.types import LongType
# create udf with return type LongType
powerUdf = udf(power, LongType())

# powerUdf can be used with dataframe python code

# we must register udf in spark session
# udf too private within spark session, udf registered in spark session not avaialble in another spark session
# "power" is udf function name, can be used sql
spark.udf.register("power", powerUdf)

# COMMAND ----------

# power is udf function registered in above cell
spark.sql("SELECT power(5)").show()

# COMMAND ----------

# Databricks notebook source
# Databricks notebook source
orders = [ 
          # (product_id, product_name, brand_id, price, qty, discount, taxp)  
         (1, 'iPhone', 100, 1000, 2, 5, 18),
         (2, 'Galaxy', 200, 800, 1, 8, 22),

]
 


orderDf = spark.createDataFrame(data=orders, schema=["product_id", "product_name", "brand_id", "price", "qty", "discount", "taxp"])
orderDf.show()

# COMMAND ----------

# UDF to calculate amount
# amount = ( price * qty ) * apply discount * taxp

def calculateAmount(price, qty, discount, taxp):
    a = price * qty
    a = a - (a * discount/100) # discounted price
    amount = a + a * taxp / 100 # with tax
    print ("amount is" , amount) 
    return amount

print(calculateAmount(1000, 2, 5, 18))

# COMMAND ----------

from pyspark.sql.types import DoubleType
# udf function, return DoubleType
calculate = udf(calculateAmount, DoubleType())
# "calculate" is used in spark sql SELECT calculate(...)
spark.udf.register("calculate", calculate)

# COMMAND ----------

# use udf in data frame, usd can be applied on dataframe or in sql 
from pyspark.sql.functions import col
# calculate is udf fucntion udf(...)
df = orderDf.withColumn("amount", calculate( col("price"), col("qty"), col("discount"), col("taxp")))
df.printSchema()
df.show()

# COMMAND ----------

# create a temp table/view
orderDf.createOrReplaceTempView("orders")

# COMMAND ----------

## now apply udf on spark sql

df = spark.sql("SELECT *, calculate(price, qty, discount, taxp) as amount from orders")
df.printSchema()
df.show()

# COMMAND ----------

