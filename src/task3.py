from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, when, round

# Initialize Spark Session
spark = SparkSession.builder.appName("SentimentByDemographics").getOrCreate()

# Load data
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("input/users.csv")

# Join posts with users to get demographic columns
joined_df = posts_df.join(users_df, on="UserID", how="inner")

# Group by Country and AgeGroup, count positive/neutral/negative posts using when()
sentiment_breakdown = (
    joined_df
    .groupBy("Country", "AgeGroup")
    .agg(
        count("PostID").alias("total_posts"),
        round(avg("SentimentScore"), 4).alias("avg_sentiment"),
        round(avg(col("Likes") + col("Retweets")), 2).alias("avg_engagement"),
        sum(when(col("SentimentScore") >  0.3,  1).otherwise(0)).alias("positive_posts"),  # score > 0.3  → Positive
        sum(when(col("SentimentScore") < -0.3,  1).otherwise(0)).alias("negative_posts"),  # score < -0.3 → Negative
        sum(when((col("SentimentScore") >= -0.3) &
                 (col("SentimentScore") <=  0.3), 1).otherwise(0)).alias("neutral_posts")  # in between   → Neutral
    )
    .orderBy(col("avg_sentiment").desc(), col("total_posts").desc())
)

sentiment_breakdown.show(30, truncate=False)

# Save result
sentiment_breakdown.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_by_demographics.csv", header=True)
