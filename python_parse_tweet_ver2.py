import json
import tweepy
import sys
import csv
from datetime import date, datetime
from pytz import timezone
from pathlib import Path

def parse_tweets(sys_args):

	file_name = sys_args[1]
	time_zone = sys_args[2] if len(sys_args) > 2 else "utc"
	keep_rt = sys_args[3] if len(sys_args) > 3 else True
	save_file_name = sys_args[4] if len(sys_args) > 4 else ('%s_parsed.csv' % file_name)
	check_duplicates = sys_args[5] if len(sys_args) > 5 else False
        Path("./results/").mkdir(parents=True, exist_ok=True)
	
	with open("./results/%s.json" % file_name, mode='r', encoding="utf-8") as tweet_data:		
		with open('./results/%s.csv' % save_file_name, mode='a', encoding="utf-8",newline='') as file:

			writer = csv.writer(file)

			if keep_rt == "True":
				writer.writerow(["tweet_id","text", "hashtags", "user_mentions", "created_at", "is_retweet", "user_screen_name", 
					"user_description", "user_following_count", "user_followers_count", "user_total_tweets", "user_created_at"])
			else:				
				writer.writerow(["tweet_id","text", "hashtags", "user_mentions", "created_at", "user_screen_name", 
					"user_description", "user_following_count", "user_followers_count", "user_total_tweets", "user_created_at"])

			tweets = json.load(tweet_data)

			for tweet in tweets["objects"]:

				tweet_id = tweet["id"]
				entities = tweet["entities"]

				user = tweet["user"]
				user_screen_name = user["screen_name"]
				user_description = user["description"]
				user_following_count = user["friends_count"]
				user_followers_count = user["followers_count"]
				user_total_tweets = user["statuses_count"]
				user_created_at = localize_utc_object(user["created_at"],time_zone)
				created_at = localize_utc_object(tweet["created_at"],time_zone)
				retweet_count = tweet["retweet_count"]

				hashtags = ()
				user_mentions = ()

				if 'hashtags' in entities:
					hashtags = (hashtag["text"] for hashtag in entities["hashtags"])
				if 'user_mentions' in entities:
					user_mentions = (("@%s" % user_mention["screen_name"]) for user_mention in entities["user_mentions"])

				hashtags = ', '.join(hashtags)				
				user_mentions = ', '.join(user_mentions)

				text = (tweet["extended_tweet"]["full_text"] if "extended_tweet" in tweet 
					else tweet["full_text"] if "full_text" in tweet else tweet["text"])

				is_retweet = ("retweeted_status" in tweet)

				if keep_rt == "True":
					writer.writerow([tweet_id,text, hashtags, user_mentions, created_at, is_retweet, user_screen_name, 
						user_description, user_following_count, user_followers_count, user_total_tweets, user_created_at])
				elif is_retweet == False: 		
					writer.writerow([tweet_id,text, hashtags, user_mentions, created_at, user_screen_name, 
						user_description, user_following_count, user_followers_count, user_total_tweets, user_created_at])

	if check_duplicates == "True":
		remove_duplicate_rows(save_file_name)

	print("Finished. Saved to ./results/%s.csv" % (save_file_name))

#note, changing the timestring to match the location based on an estimation (by language filters for example) is a bit sloppy:
#-> what about Japanese tweets posted abroad? Difficult to detect since utc_offset is no longer supported by Twitter
#https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
def localize_utc_object(time_string, time_zone):
	date_object =  datetime.strptime(time_string, '%a %b %d %H:%M:%S %z %Y')
	if time_zone != "utc":	
		date_object = date_object.astimezone(tz=timezone(time_zone)) #for example Asia/Tokyo
	return date_object.isoformat()

def remove_duplicate_rows(save_file_name):
	with open('./results/%s.csv' % save_file_name,encoding="utf-8") as f:
		data = list(csv.reader(f))		
		data = list(csv.reader(f))

		for row in data:
			if row[0] not in keys:
				new_data.append(row)
				keys.append(row[0])

	with open('./results/%s.csv' % save_file_name, 'w',encoding="utf-8",newline='') as t:
		write = csv.writer(t)
		write.writerows(new_data)
		new_data = [a for i, a in enumerate(data) if a not in data[:i]]
		with open('./results/%s.csv' % save_file_name, 'w',encoding="utf-8",newline='') as t:
			write = csv.writer(t)
			write.writerows(new_data)

if __name__ == '__main__':
	parse_tweets(sys.argv)
