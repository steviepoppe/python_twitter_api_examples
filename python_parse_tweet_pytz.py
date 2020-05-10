import json
import tweepy
import sys
import csv
from datetime import date, datetime
import time
import re
from pytz import timezone

def parse_tweets(sys_args):

	file_name = sys_args[1]
	time_zone = sys_args[2] if len(sys_args) > 2 else "utc"

	with open("%s.json" % file_name, mode='r', encoding="utf-8") as tweet_data:		
		with open('%s.csv' % file_name, mode='w', encoding="utf-8",newline='') as file:

			writer = csv.writer(file)
			writer.writerow(["text", "hashtags", "created_at", "is_retweet", "user_screen_name", "user_description", "user_friends_count", "user_followers_count", "user_total_tweets", "user_created_at"])

			tweets = json.load(tweet_data)

			for tweet in tweets["objects"]:
				user_screen_name = tweet["user"]["screen_name"]
				user_description = tweet["user"]["description"]
				user_friends_count = tweet["user"]["friends_count"]
				user_followers_count = tweet["user"]["followers_count"]
				user_total_tweets = tweet["user"]["statuses_count"]
				user_created_at = tweet["user"]["created_at"]
				created_at = localize_utc_object(tweet["created_at"],time_zone).isoformat() #ISO 8601
				retweet_count = tweet["retweet_count"]
				hashtags = []
				if len(tweet["entities"]["hashtags"]) > 0:
					for hashtag in tweet["entities"]["hashtags"]:
						hashtags.append(hashtag["text"])
				#converts hashtag dict to comma-seperated string, can be commented out if original list is preferred
				hashtags = ', '.join(hashtags)
				text = (tweet["extended_tweet"]["full_text"] if "extended_tweet" in tweet else tweet["full_text"] if "full_text" in tweet else tweet["text"])
				is_retweet = ("retweeted_status" in tweet)				
				is_quote = ("quoted_status" in tweet)

				quoted_text = ("und" if not is_quote else tweet["quoted_status"]["full_text"] if "full_text" in tweet["quoted_status"] else tweet["quoted_status"]["text"])

				writer.writerow([text, hashtags, created_at, is_retweet, user_screen_name, user_description, user_friends_count, user_followers_count, user_total_tweets, user_created_at])

	print("Finished. Saved to %s_tweets.csv" % (file_name))

#note, changing the timestring to match the location based on an estimation (by language filters for example) is a bit sloppy:
#-> what about Japanese tweets posted abroad? Difficult to detect since utc_offset is no longer supported
#https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
def localize_utc_object(time_string, time_zone):
	date_object =  datetime.strptime(time_string, '%a %b %d %H:%M:%S %z %Y').now(timezone('UTC'))
	if time_zone == "utc":
		return date_object
	return date_object.astimezone(tz=timezone(time_zone)) #for example Asia/Tokyo

if __name__ == '__main__':
	#pass in the target filename without "json" as argument in command prompt.
	parse_tweets(sys.argv)