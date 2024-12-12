import os



SCALA_VERSION = '2.11'
SPARK_VERSION = '2.4.7'

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'


import findspark
findspark.init()

import pyspark

# consume from a topic called invoicess
# calculate aggregate , print data on console
# publish the aggregated values back to kafka as JSON

# kafka create a topic called "aggregated-invoices"

# kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic aggregated-invoices

# run consumer to listen on messages from aggregated-invoices
# kafka-console-consumer --bootstrap-server localhost:9092 --topic aggregated-invoices 


from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]")\
                            .appName("SparkStreamingKafkaInvoiceStream").getOrCreate()


# read from kafka, here spark is consumer for kafka topic called invoices
# spark streaming works as dataframe/sql
kafkaDf = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "invoices2")\
  .load()


# .show/print will not work directily due to stream..
# linesDf.show() # worn't work
kafkaDf.printSchema() # works

# key is kafka key, in binary format
# value is kafka value, in binary format
# topic string
# parition, integer
# offer long 
# timestamp - longint in ms
# timestampType - Source Time, Record write time

# now convert kafka value which is in bytes to STRING, we ignore the key for now...
# now we pick only value from the stream..
invoiceJsonRawDf = kafkaDf.selectExpr("timestamp", "CAST(value AS STRING)")
invoiceJsonRawDf.printSchema() # we get only value as string


import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType
# json is object, spark DF needs schema 

schema = StructType(
        [
            StructField("InvoiceNo", IntegerType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("Description", StringType(), True),
            StructField("InvoiceDate", StringType(), True),
            #StructField("InvoiceDate", DateType(), True),
            StructField("UnitPrice", DoubleType(), True),
            StructField("CustomerID", IntegerType(), True),
            StructField("Country", StringType(), True),
        ]
)

#{"InvoiceNo": 495774, "StockCode": "84406G", "Quantity": 2, "Description": "TODO", "InvoiceDate": "05/22/2021 00:36", "UnitPrice": 2.0, "CustomerID": 17850, "Country": "AT"}

# replacing json string with a json object with schema
# now value is a column, it contains a struct
jsonDf = invoiceJsonRawDf.withColumn("value", F.from_json("value", schema))

# now we will extract value which struct type ewith all schema field mention, to specific columns
#InvoiceNo, StockCode, ....
invoiceDf = jsonDf.select("timestamp", F.col("value.*")) 

# run cell by cell, than all...
# dataframe specific, raw 
echoOnconsole = invoiceDf\
                .writeStream\
                .outputMode("append")\
                .format("console")\
                .start() # start the query. spark will subscribe for data

filteredDf = invoiceDf.filter("Quantity >= 6") 

echoOnconsole = filteredDf\
                .writeStream\
                .outputMode("append")\
                .format("console")\
                .start() # start the query. spark will subscribe for data

# groupby for count, unique items, not count of Quantity 
# 2 apples, 3 orangles = answer 2
groupByItemCount = invoiceDf.groupBy("InvoiceNo").count()

echoOnconsole = groupByItemCount\
                .writeStream\
                .outputMode("complete")\
                .format("console")\
                .start() # start the query. spark will subscribe for data


# Give me how much money generated selling goods for last 60 seconds/1 minutes
invoiceDf = invoiceDf.withColumn("Amount", F.col("Quantity") * F.col("UnitPrice") )
# below code is not right solution, group by, by last last 6o seconds
# groupByItemCount = invoiceDf.groupBy("InvoiceNo")....

windowedAmountSum = invoiceDf.groupBy(F.window(invoiceDf.timestamp, 
                                              "60 seconds", 
                                               "60 seconds"), invoiceDf.Amount).sum()

echoOnconsole = windowedAmountSum\
                .writeStream\
                .outputMode("complete")\
                .format("console")\
                .start() # start the query. spark will subscribe for data

# Give me how much money generated selling goods for last 60 seconds/1 minutes
invoiceDf = invoiceDf.withColumn("Amount", F.col("Quantity") * F.col("UnitPrice") )
# below code is not right solution, group by, by last last 6o seconds
# groupByItemCount = invoiceDf.groupBy("InvoiceNo")....

windowedAmountSum = invoiceDf.groupBy(F.window(invoiceDf.timestamp, 
                                              "60 seconds", 
                                               "60 seconds"))\
                              .agg(F.sum("Amount"))

echoOnconsole = windowedAmountSum\
                .writeStream\
                .outputMode("complete")\
                .format("console")\
                .start() # start the query. spark will subscribe for data

echoOnconsole.awaitTermination()

# later you can terminal the jupyter               