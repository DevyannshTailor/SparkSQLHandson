from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# TODO: Split the Hashtags column into individual hashtags and count the frequency of each hashtag and sort descending
hashtag_counts = (
    posts_df
    .select(explode(split(col("Hashtags"), ",")).alias("hashtag"))  # split comma-separated hashtags into individual rows
    .groupBy("hashtag")                                             # group by each unique hashtag
    .count()                                                        # count how many times each appears
    .orderBy(col("count").desc())                                   # sort highest frequency first
)

# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)
