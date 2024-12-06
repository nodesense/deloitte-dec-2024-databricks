#import findspark
#findspark.init()

# this block is called cell

# to run the cell, SHIFT + ENTER

from pyspark.conf import SparkConf
config = SparkConf()
# config.set("property", "value")
config.setMaster("local").setAppName("MovieLens")

from pyspark.sql import SparkSession
# spark Session, entry point for Spark SQL, DataFrame
spark = SparkSession.builder\
                    .config(conf=config)\
                    .getOrCreate()

sc = spark.sparkContext

# how to create schema programatically instead of using inferSchema
from pyspark.sql.types import StructType, LongType, StringType, IntegerType, DoubleType
# True is nullable, False is non nullable
movieSchema = StructType()\
                .add("movieId", IntegerType(), True)\
                .add("title", StringType(), True)\
                .add("genres", StringType(), True)

ratingSchema = StructType()\
                .add("userId", IntegerType(), True)\
                .add("movieId", IntegerType(), True)\
                .add("rating", DoubleType(), True)\
                .add("timestamp", LongType(), True)

# read movie data
# read using dataframe with defind schema
# we can use folder path - all csv in the folder read
# use file path, only that file read

# spark is session, entry point for data frame/sql

movieDf = spark.read.format("csv")\
                .option("header", True)\
                .schema(movieSchema)\
                .load("s3://deloitte-mar2023/movies/")

movieDf.printSchema()
movieDf.show(2)

ratingDf = spark.read.format("csv")\
                .option("header", True)\
                .schema(ratingSchema)\
                .load("s3://deloitte-mar2023/ratings/")

ratingDf.printSchema()
ratingDf.show(2)

print (movieDf.count())
print(ratingDf.count())

ratingDf.take(2)

# show the distinct ratings
ratingDf.select("rating").distinct().show()

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find the movies by total ratings by userId
df = ratingDf\
     .groupBy("movieId")\
     .agg(count("userId").alias("total_ratings"))\
     .sort(desc("total_ratings"))

df.printSchema()
df.show(20)

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find  average rating by users sorted by desc
df = ratingDf\
     .groupBy("movieId")\
     .agg(avg("rating").alias("avg_rating"))\
     .sort(desc("avg_rating"))

df.printSchema()
df.show(20)

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find  the most popular movies, where as rated by many users, at least movies should be rated by 100 users
# and the average rating should be at least 3.5 and above
# and sort the movies by total_ratings
mostPopularMoviesDf = ratingDf\
     .groupBy("movieId")\
     .agg(avg("rating").alias("avg_rating"), count("userId").alias("total_ratings") )\
     .filter( (col("total_ratings") >= 100) & (col("avg_rating") >=3.5) )\
     .sort(desc("total_ratings"))

mostPopularMoviesDf.cache() # MEMORY

mostPopularMoviesDf.printSchema()
mostPopularMoviesDf.show(20)

mostPopularMoviesDf.explain(extended=True)

# join, inner join 
# get the movie title for the mostPopularMoviesDf
# join mostPopularMoviesDf with movieDf based on condition that mostPopularMoviesDf.movieId == movieDf.movieId

popularMoviesDf = mostPopularMoviesDf.join(movieDf, mostPopularMoviesDf.movieId == movieDf.movieId)\
                                     .select(movieDf.movieId, "title", "avg_rating", "total_ratings")\
                                     .sort(desc("total_ratings"))

popularMoviesDf.cache()

popularMoviesDf.show(100)
