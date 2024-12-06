# Databricks notebook source
products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), #   no matching brand
         (4, 'Pixel', 400),
]

brands = [
    #(brand_id, brand_name)
    (100, "Apple"),
    (200, "Samsung"),
    (400, "Google"),
    (500, "Sony"), # no matching products
]
 
productDf = spark.createDataFrame(data=products, schema=["product_id", "product_name", "brand_id"])
brandDf = spark.createDataFrame(data=brands, schema=["brand_id", "brand_name"])
productDf.show()
brandDf.show()

# temp table or temp view created within spark session
# create temp table out of data frame, which can be used in spark sql
productDf.createOrReplaceTempView("products")
brandDf.createOrReplaceTempView("brands")

# COMMAND ----------

# spark sql using spark session
# case insensitive 
df = spark.sql("SELECT  * FROM products") # spark sql returns data frame [lazy code]
df.printSchema()
df.show()

# COMMAND ----------

# Inner Join
# productDf is left
# brandDf is right
# inner join: select/pick only matching record, discord if no matches found
# join returns a new df
df = productDf.join(brandDf, productDf.brand_id  ==  brandDf.brand_id, "inner") 
df.printSchema()
df.show()

# COMMAND ----------

# inner join in sql syntax
# products and brands are temp tables/views
# products.* - project all columns
# alogn with brand_name from brands table 
spark.sql("""
SELECT products.*, brands.brand_name FROM products
INNER JOIN brands ON products.brand_id = brands.brand_id
""").show()


# COMMAND ----------

# how to remove specific column, when the same column present in two dataframe
from pyspark.sql.functions import col
# using col("brand_id") will have  Reference 'brand_id' is ambiguous,   since brand_id available from 2 dfs
# drop(brandDf["brand_id"]) drop column from brandDf
# drop(productDf.brand_id) drop column from productDf

df = productDf.join(brandDf, productDf.brand_id  ==  brandDf["brand_id"], "inner")\
              .drop(brandDf["brand_id"])

df.printSchema()
df.show()

# COMMAND ----------

# Outer Join, Full Outer Outer, [Left outer + Right outer]
# pick all records from left dataframe, and also right dataframe
# if no matches found, it fills null data for not matched records
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "outer").show()

# COMMAND ----------

# outer join in sql
spark.sql("""
SELECT products.*, brands.* FROM products
FULL OUTER JOIN brands ON products.brand_id = brands.brand_id
""").show()

# COMMAND ----------

# Left, Left Outer join 
# picks all records from left, if no matches found, it fills null for right data
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "leftouter").show()

# COMMAND ----------

spark.sql("""
SELECT products.*, brands.* FROM products
LEFT OUTER JOIN brands ON products.brand_id = brands.brand_id
""").show()

# COMMAND ----------

# Right, Right outer Join
# picks all the records from right, if no matches found, fills left data with null
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "rightouter").show()

# COMMAND ----------

spark.sql("""
SELECT products.*, brands.* FROM products
RIGHT OUTER JOIN brands ON products.brand_id = brands.brand_id
""").show()

# COMMAND ----------

# left semi join
# join in general convention, it pull the records from both right and left, join them based on condition
# left semi join, join left and right based on condition, however it project the records only from left side

# it is similar to innerjoin, but pick/project records only from left
# we can't see brand_id, brand_name from brands df
# result will not include not matching data [similar to inner join]
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "leftsemi").show()

# COMMAND ----------

spark.sql("""
SELECT products.* FROM products
LEFT SEMI JOIN brands ON products.brand_id = brands.brand_id
""").show()

# COMMAND ----------

# left anti join: exact opposite to semi join
# picks the records that doesn't have match on the right side
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "leftanti").show()

# COMMAND ----------

spark.sql("""
SELECT products.* FROM products
LEFT ANTI   JOIN brands ON products.brand_id = brands.brand_id
""").show()

# COMMAND ----------

store = [
    #(store_id, store_name)
    (1000, "Poorvika"),
    (2000, "Sangeetha"),
    (4000, "Amazon"),
    (5000, "FlipKart"), 
]
 
storeDf = spark.createDataFrame(data=store, schema=["store_id", "store_name"])
storeDf.show()
storeDf.createOrReplaceTempView("stores")

# COMMAND ----------

# cartesian , take row from left side, pair with all from right side
productDf.crossJoin(storeDf).show()

# COMMAND ----------

spark.sql("""
SELECT products.*, brands.* FROM products
CROSS JOIN brands
""").show()

# COMMAND ----------

