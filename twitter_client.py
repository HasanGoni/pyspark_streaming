# Import libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# import configuration
from config import PORT_NUMBER

spark = SparkSession.builder\
       .appName("TwitterStream")\
       .getOrCreate()


# read tweet data from socket

tweet_df = spark\
           .readStream\
           .format('socket')\
           .option('host', '127.0.0.1')\
           .option('port', PORT_NUMBER)\
           .load()

# Casting as string 
tweet_df_string = tweet_df.selectExpr(
    "CAST(value AS STRING)"
                                    )

# spliting with with white space 
# then groupby based on this  
# counting 
# sorting based on count
# filtering only based on hashtag

tweets_tab = tweet_df_string.withColumn(
    "word",
    explode(split(col('value'), ' ')))\
    .groupby('word')\
    .count()\
    .sort('count',ascending=False)\
    .filter(col('word').contains('#'))


# writing now on memory

writeTweet = tweets_tab.writeStream\
             .outputMode('complete')\
             .format('memory')\
             .queryName("tweetquery")\
             .trigger(processingTime='5 seconds')\
             .start()
print('==== Streaming is running, to stop press contrl c =====')

for i in range (5):
    spark.sql("select * from tweetquery").show(9)
    
writeTweet.stop()
print(f' status of the stream == {writeTweet.status}\n')
print(f' Write tweet is aciver or not == {writeTweet.isActive}\n')



                            