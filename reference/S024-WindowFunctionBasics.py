# Databricks notebook source
data = [ ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100),
    ("Joe", "Sales", 4200),
    ("Venkat", "Sales", 4000),
        
   ]

empDf = spark.createDataFrame(data=data, schema=['name', 'dept', 'salary'])
empDf.printSchema()
empDf.show()

empDf.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC # Hive Partition 
# MAGIC 
# MAGIC 1.  not spark partitions
# MAGIC 2. way of storing data in categorical or date time etc
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC sales/
# MAGIC     year=2022/
# MAGIC       month=07/
# MAGIC         day=06/
# MAGIC             datafile1.csv
# MAGIC             datafile2.csv
# MAGIC         day=07/
# MAGIC             datafile1.csv
# MAGIC             datafile2.csv..  
# MAGIC             
# MAGIC sales-2/
# MAGIC      country=usa
# MAGIC        state=CA
# MAGIC           datafile1.csv
# MAGIC           datafile2.csv
# MAGIC        state=ny
# MAGIC            datafile.....
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. This partition schema is based off hive standard, supported by hive, spark, presto, flink and many other data processign engines
# MAGIC 2. This is to improve performance of queries
# MAGIC 3. SELECT * from SALES where year=2022 and month=07 and day=06 
# MAGIC     This query only read the files stored in sales/year=2022/month=07/day=06/*.csv directory, doesn't read from other directory

# COMMAND ----------

empDf.rdd.glom().collect()

# COMMAND ----------

# try to write the dataframe into csv using partition based on hive standard, partition column is dept
# dept=Finance, dept=Marketing, dept=Sales, the records of those are stored in separate directories

empDf.write.option("header", True)\
  .partitionBy("dept")\
  .csv("/FileStore/tables/july2022/employees") # with in employees, we can see files with specific department which stores dept specific staffs


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
# Window functions: window functions are applied on specific partitioned data, to find relative ranking, order, row number etc on given data partition
# row_number is a window function, assign numbers from 1 ...n for each record in the data partition
# if you want get 5 records of each dept [partition by dept]
# specification for window, partitions, functions that should be applied on partition
# with in department, order the data based on salary in ascending order
windowSpec = Window.partitionBy("dept").orderBy("salary")
# we have apply the spec on dataframe
df = empDf.withColumn("slno", row_number().over(windowSpec))

df.printSchema()
df.show()

df.filter (df.slno == 1).show()

# COMMAND ----------

df.rdd.glom().collect()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# rank with gap with ascending order
"""
score  rank
90      1
90      1
89      3  [gap, 2 not included]
"""
windowSpec = Window.partitionBy("dept").orderBy("salary")

df = empDf.withColumn("rank", rank().over(windowSpec))
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import first, last

#  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

windowSpec = Window.partitionBy("dept").orderBy("salary")\
                 
# without unboundedFollowing, it takes current row under processing as last record

df = empDf.withColumn("first", first("salary").over(windowSpec))\
          .withColumn("last", last("salary").over(windowSpec))
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import first, last
 
windowSpec = Window.partitionBy("dept").orderBy("salary")\
                   .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
                 
# with  unboundedFollowing, it iterates all the records next, take last record as last record for all records
# with unboundedPreceding, it iterates all records previous, so take first record as first record for all

df = empDf.withColumn("first", first("salary").over(windowSpec))\
          .withColumn("last", last("salary").over(windowSpec))
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# rank with gap
"""
score  rank
90      1
90      1
89      3  [gap, 2 not included]
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("rank", rank().over(windowSpec))
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc

# dense_rank ranking without gap
"""
score  rank
90      1
90      1
89      2  
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("rank", dense_rank().over(windowSpec))
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank, desc

# percent_rank ranking with perecent calculation
# similar to rank, but it is calculated from 0 to 1, gap applied if same rank used many times
"""
 
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("rank", percent_rank().over(windowSpec))
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import ntile, desc

# ntile  with related certain range for range

"""
 
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("tile", ntile(4).over(windowSpec))
df.show()

# COMMAND ----------

# Analytic functions
# Cumulative distribution 
# applied between 0 and 1

# 10 USD per share => 13 USD per share      = 3 USD per share, 30 % gain .3
# 100 USD per share => 110 USD per share    = 10 USD per share, 10% gain .1
# cumulative distribution
from pyspark.sql.window import Window
from pyspark.sql.functions import cume_dist, desc

# similar to  rank  
 
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("cume_dist", cume_dist().over(windowSpec))
df.show()

# COMMAND ----------

# lag - previous lag
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, desc

windowSpec = Window.partitionBy("dept").orderBy("salary")

df = empDf.withColumn("lag", lag("salary",1).over(windowSpec))
df.show()

# COMMAND ----------

# lead -  the one ahead, 
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, desc

windowSpec = Window.partitionBy("dept").orderBy("salary")

df = empDf.withColumn("lead", lead("salary", 1).over(windowSpec))
df.show()

# COMMAND ----------

# aggregate functions, min, max, sum, count, avg

from pyspark.sql.window import Window
from pyspark.sql.functions import avg, sum, min, max, count, col

windowSpec = Window.partitionBy("dept")

df = empDf\
          .withColumn("min", min(col("salary")).over(windowSpec))\
          .withColumn("max", max(col("salary")).over(windowSpec))\
          .withColumn("avg", avg(col("salary")).over(windowSpec))\
          .withColumn("count", count(col("salary")).over(windowSpec))\
          .withColumn("sum", sum(col("salary")).over(windowSpec))

df.show()

# COMMAND ----------

empDf.createOrReplaceTempView("employees")

# COMMAND ----------

# sql

spark.sql("""
SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY SALARY) as slno FROM employees
""").show()

# COMMAND ----------

# sql

spark.sql("""
SELECT name, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY SALARY) as rank FROM employees
""").show()

# COMMAND ----------

# sql

spark.sql("""
SELECT name, dept, salary, 

        FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY SALARY 
                                RANGE BETWEEN UNBOUNDED PRECEDING AND 
                                UNBOUNDED FOLLOWING) as first ,
                                
        LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY SALARY 
                                RANGE BETWEEN UNBOUNDED PRECEDING AND 
                                UNBOUNDED FOLLOWING) as last 
                                
                                FROM employees
""").show()

# COMMAND ----------

# sql

spark.sql("""
SELECT name, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY SALARY DESC) as rank FROM employees
""").show()

# COMMAND ----------

