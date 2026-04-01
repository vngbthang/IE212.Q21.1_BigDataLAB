import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext

movies_rdd = sc.textFile("file:///home/vngbthang/IE212.Q21.1_BigDataLAB/LAB3/movies.txt")

movies = movies_rdd.collect()

for movie in movies:
    print(movie)
spark.stop()