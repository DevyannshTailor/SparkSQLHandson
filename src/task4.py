from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when, round
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Initialize Spark Session
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load data
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("input/users.csv")

# Filter to verified users only, then join with posts
verified_users_df = users_df.filter(col("Verified") == True)
joined_df = posts_df.join(verified_users_df, on="UserID", how="inner")

# Aggregate per verified user with a weighted engagement score (Retweets worth 2x — they amplify reach)
aggregated = (
    joined_df
    .groupBy("UserID", "Username", "AgeGroup", "Country")
    .agg(
        count("PostID").alias("total_posts"),
        sum("Likes").alias("total_likes"),
        sum("Retweets").alias("total_retweets"),
        (sum("Likes") + sum(col("Retweets") * 2)).alias("weighted_score"),   # weighted engagement score
        round(avg("SentimentScore"), 4).alias("avg_sentiment"),
        sum(when(col("SentimentScore") >  0.3,  1).otherwise(0)).alias("positive_posts"),
        sum(when(col("SentimentScore") < -0.3,  1).otherwise(0)).alias("negative_posts"),
        sum(when((col("SentimentScore") >= -0.3) &
                 (col("SentimentScore") <=  0.3), 1).otherwise(0)).alias("neutral_posts")
    )
)

# Use a Window function to rank users by weighted_score
window_spec = Window.orderBy(col("weighted_score").desc())

top_verified_users = (
    aggregated
    .withColumn("engagement_rank", rank().over(window_spec))  # RANK() assigns position 1 = highest score
    .filter(col("engagement_rank") <= 10)                     # keep only top 10
    .orderBy("engagement_rank")
)

top_verified_users.show(truncate=False)

# Save result
top_verified_users.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)
