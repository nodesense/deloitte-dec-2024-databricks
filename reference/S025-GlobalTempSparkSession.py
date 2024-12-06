# Databricks notebook source
# Databricks notebook source
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
productDf.show()

brandDf = spark.createDataFrame(data=brands, schema=["brand_id", "brand_name"])

brandDf.show()

store = [
    #(store_id, store_name)
    (1000, "Poorvika"),
    (2000, "Sangeetha"),
    (4000, "Amazon"),
    (5000, "FlipKart"), 
]
 
storeDf = spark.createDataFrame(data=store, schema=["store_id", "store_name"])
storeDf.show()

# COMMAND ----------

# in any spark application, there will be ONLY ONE spark context
# and as many spark sessions allowed

spark2 = spark.newSession()

# COMMAND ----------

# we created productDf using spark.createDataFrame
# create product temp table in spark session
# products is temp view, private to spark session, means we cannot access from spark 2
# productDf = spark.createDataFrame(...), productDf is part spark session
# temp table created shall go to spark session
productDf.createOrReplaceTempView("products")

# COMMAND ----------

# now access products from session /knew it will work
spark.sql("SELECT * FROM products").show()

# COMMAND ----------

# now try to access products from spark2, IT WILL FAIL, as products table private to spark session
spark2.sql("SELECT * FROM products").show() # error  AnalysisException: Table or view not found: products; 

# COMMAND ----------

# now create global temp view global_temp that can be shared across multiple sessions on same notebook
# spark has a database global_temp [built in]
# create a table in global_temp.brands
brandDf.createOrReplaceGlobalTempView ("brands")

spark.sql("SHOW TABLES IN global_temp").show()

# COMMAND ----------

# MUST prefix global_temp to access global temp view
spark.sql("SELECT * FROM global_temp.brands").show()

# COMMAND ----------

# brands from global temp will be displayed, but products table not listed as it was created for spark session, not for spark2 session
spark2.sql("SHOW TABLES IN global_temp").show()

# COMMAND ----------

# access global temp from spark2 session
spark2.sql("SELECT * FROM global_temp.brands").show()

# COMMAND ----------

# DIY
# we have store data, create storeDf2 dataframe on spark2 session, create a temp view called stores in spark2 session and list content


storeDf2 = spark2.createDataFrame(data=store, schema=["store_id", "store_name"])
storeDf2.show()

storeDf2.createOrReplaceTempView("stores")

spark2.sql("SHOW TABLES").show()

# COMMAND ----------

spark2.sql("SELECt * FROM stores").show()

# COMMAND ----------

spark2.sql("SHOW TABLES in global_temp").show()

# COMMAND ----------

# Drop table from global_temp
spark2.sql("DROP TABLE global_temp.brands").show()

# COMMAND ----------

# brands won't be listed, since it is removed.
spark2.sql("SHOW TABLES in global_temp").show()

# COMMAND ----------

