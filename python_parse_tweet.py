import json
import tweepy
import sys
import csv
from pathlib import Path
from datetime import date, datetime, timezone


def parse_tweets(file_name):

        Path("./results/").mkdir(parents=True, exist_ok=True)
	with open("./results/%s.json" % file_name, mode='r', encoding="utf-8") as tweet_data:		
		with open('./results/%s.csv' % file_name, mode='w', encoding="utf-8",newline='') as file:

			writer = csv.writer(file)

			writer.writerow(["tweet_id","text", "hashtags", "created_at", "is_retweet", "user_screen_name", 
				"user_description", "user_friends_count", "user_followers_count", 
				"user_total_tweets", "user_created_at"])

			tweets = json.load(tweet_data)

			for tweet in tweets["objects"]:
				
				tweet_id = tweet["id"]
				entities = tweet["entities"]

				user = tweet["user"]
				user_screen_name = user["screen_name"]
				user_description = user["description"].strip()
				user_following_count = user["friends_count"]
				user_followers_count = user["followers_count"]
				user_total_tweets = user["statuses_count"]
				user_created_at = string_to_dt(user["created_at"])
				created_at = string_to_dt(tweet["created_at"])
				retweet_count = tweet["retweet_count"]

				is_retweet = ("retweeted_status" in tweet)				
				#is_quote = ("quoted_status" in tweet)

				hashtags = ()

				if is_retweet == True:
					retweet = tweet["retweeted_status"]
					retweet_original_id = retweet["id"]
					re_entities = retweet["entities"]

					text = "RT @" + entities["user_mentions"][0]["screen_name"] + ": " + (retweet["extended_tweet"]["full_text"] 
						if "extended_tweet" in retweet else retweet["full_text"] if "full_text" in retweet else retweet["text"])
					
					if 'hashtags' in re_entities:
						hashtags = (hashtag["text"] for hashtag in re_entities["hashtags"])
				else:					
					text = (tweet["extended_tweet"]["full_text"] if "extended_tweet" in tweet 
					else tweet["full_text"] if "full_text" in tweet else tweet["text"])		

					if 'hashtags' in entities:
						hashtags = (hashtag["text"] for hashtag in entities["hashtags"])

				#converts hashtag dict to comma-seperated string, can be commented out if original list is preferred
				hashtags = ', '.join(hashtags)

				text = text.strip()

				#quoted_text = ("und" if not is_quote else tweet["quoted_status"]["full_text"] 
				#	if "full_text" in tweet["quoted_status"] else tweet["quoted_status"]["text"])

				writer.writerow([tweet_id, text, hashtags, created_at, is_retweet, user_screen_name, user_description, 
					user_following_count, user_followers_count, user_total_tweets, user_created_at])

	print("Finished. Saved to ./results/%s_tweets.csv" % (file_name))

#converts Tweet date to ISO 8601 compliant string. Tweet timezones are standard UTC
def string_to_dt(time_string):
	date_object =  datetime.strptime(time_string, '%a %b %d %H:%M:%S %z %Y')
	return date_object.isoformat()

if __name__ == '__main__':
	#pass in the target filename without "json" as argument in command prompt.
	parse_tweets(sys.argv[1])
