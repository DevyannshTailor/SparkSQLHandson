from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when, round

# Initialize Spark Session
spark = SparkSession.builder.appName("UserEngagement").getOrCreate()

# Load data
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("input/users.csv")

# Join posts with users on UserID
joined_df = posts_df.join(users_df, on="UserID", how="inner")

# Aggregate per user: total posts, likes, retweets, total engagement, avg sentiment, sentiment label
user_engagement = (
    joined_df
    .groupBy("UserID", "Username", "AgeGroup", "Country", "Verified")
    .agg(
        count("PostID").alias("total_posts"),                          # how many posts each user made
        sum("Likes").alias("total_likes"),                             # sum of all likes
        sum("Retweets").alias("total_retweets"),                       # sum of all retweets
        (sum("Likes") + sum("Retweets")).alias("total_engagement"),    # combined engagement score
        round(avg("SentimentScore"), 4).alias("avg_sentiment")         # average sentiment across posts
    )
    .withColumn(                                                        # classify user's overall sentiment
        "sentiment_label",
        when(col("avg_sentiment") >  0.3, "Positive")
       .when(col("avg_sentiment") < -0.3, "Negative")
       .otherwise("Neutral")
    )
    .orderBy(col("total_engagement").desc())                           # rank by highest engagement first
)

user_engagement.show(20, truncate=False)

# Save result
user_engagement.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)
