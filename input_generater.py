import csv
import os
import random
from datetime import datetime, timedelta

# Create input directory if it doesn't exist
os.makedirs("input", exist_ok=True)

# Sample users
user_data = []
usernames = [
    "@techie42", "@critic99", "@daily_vibes", "@designer_dan", "@rage_user",
    "@meme_lord", "@social_queen", "@calm_mind", "@pixel_pusher", "@stream_bot"
]
age_groups = ["Teen", "Adult", "Senior"]
countries = ["US", "UK", "Canada", "India", "Germany", "Brazil"]
verified_status = [True, False]

for user_id in range(1, 51):
    user = {
        "UserID": user_id,
        "Username": random.choice(usernames) + str(user_id),
        "AgeGroup": random.choice(age_groups),
        "Country": random.choice(countries),
        "Verified": random.choice(verified_status)
    }
    user_data.append(user)

# Write users.csv
with open("input/users.csv", mode="w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=user_data[0].keys())
    writer.writeheader()
    writer.writerows(user_data)

# Sample posts
hashtags_pool = ["#tech", "#fail", "#design", "#UX", "#cleanUI", "#mood", "#bug", "#love", "#social", "#AI"]
contents = [
    "Loving the new update!",
    "This app keeps crashing. So annoying.",
    "Just another day...",
    "Absolutely love the UX!",
    "Worst experience ever.",
    "Such a smooth interface!",
    "Great performance on mobile.",
    "Can’t stop using it!",
    "Needs dark mode ASAP!",
    "I’m impressed with the speed."
]

posts_data = []
base_time = datetime.now()

for post_id in range(101, 201):
    uid = random.randint(1, 10)
    timestamp = (base_time - timedelta(hours=random.randint(0, 240))).strftime("%Y-%m-%d %H:%M:%S")
    content = random.choice(contents)
    likes = random.randint(0, 150)
    retweets = random.randint(0, 50)
    sentiment = round(random.uniform(-1, 1), 2)
    hashtags = ",".join(random.sample(hashtags_pool, random.randint(1, 3)))

    post = {
        "PostID": post_id,
        "UserID": uid,
        "Content": content,
        "Timestamp": timestamp,
        "Likes": likes,
        "Retweets": retweets,
        "Hashtags": hashtags,
        "SentimentScore": sentiment
    }
    posts_data.append(post)

# Write posts.csv
with open("input/posts.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=posts_data[0].keys())
    writer.writeheader()
    writer.writerows(posts_data)

print("✅ Dataset generation complete: 'users.csv' and 'posts.csv' created in /input/")
