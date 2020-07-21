import json
import sys
import csv
from datetime import date, datetime
import time
from pathlib import Path
import pandas as pd

# This script takes the CSV file generated with python_parse_tweet_ver2.py as input and generates several CSV files with metrics we might use in Excel or any spreadsheet software
# Again it uses the third-party library pandas to read particularly large (>100mb) CSV files in chunks, and to save to CSV the above metrics
# For an example on how to use this, see https://steviepoppe.net/blog/2020/05/a-quick-guide-to-data-mining-textual-analysis-of-japanese-twitter-part-2/

def parse_tweets(sys_args):

	file_name = sys_args[1]
	keep_rt = sys_args[2] if len(sys_args) > 2 else "True"
	chunksize = 100000
	hashtags = {}
	date_set = {}
	time_set = {}
	user_set = {}
	line_count = 0
	
	Path("./results/metrics_%s/" % file_name).mkdir(parents=True, exist_ok=True)

	for chunk in pd.read_csv('./results/%s.csv' % file_name, encoding="utf-8", chunksize=chunksize, iterator=True):
		for index, tweet in chunk.iterrows():
			line_count += 1
			is_retweet = 1 if tweet["is_retweet"] == True else 0
			hashtag_metrics(tweet, hashtags, is_retweet)
			date_metrics(tweet, date_set, is_retweet)
			time_metrics(tweet, time_set, is_retweet)
			user_metrics(tweet, user_set, is_retweet, keep_rt)
		print('Processed %s lines.' % line_count)
		
	print('Processed total of %s lines.' % line_count)
	save_hashtag_metrics(hashtags, file_name)
	save_date_metrics(date_set, file_name)
	save_time_metrics(time_set, file_name)
	save_user_metrics(user_set, file_name)
			
def hashtag_metrics(tweet, hashtags, is_retweet):
	if not pd.isna(tweet["hashtags"]):
		c_hashtags = tweet["hashtags"].replace(",,",",").split(",")
		for hashtag in c_hashtags:						
			if hashtag != '':
				if hashtag in hashtags:
					hashtags[hashtag][is_retweet] = hashtags[hashtag][is_retweet] + 1
				else:
					retweet_status = [0,0,0]
					retweet_status[is_retweet] = 1
					retweet_status[not is_retweet] = 0
					retweet_status[2] = [0,0]
					retweet_status[2][0] = []
					retweet_status[2][1] = []
					hashtags[hashtag] = retweet_status
				hashtags[hashtag][2][is_retweet].append(tweet["user_screen_name"])

def date_metrics(tweet, date_set, is_retweet):
	tweet_created_date = datetime.fromisoformat(tweet["created_at"]).strftime("%m/%d/%Y")
	if not tweet_created_date in date_set:
		retweet_status = [0,0,0]
		retweet_status[is_retweet] = 1
		retweet_status[not is_retweet] = 0
		date_set[tweet_created_date] = retweet_status
		retweet_status[2] = [0,0]
		retweet_status[2][0] = []
		retweet_status[2][1] = []
	else:
		date_set[tweet_created_date][is_retweet] += 1
	date_set[tweet_created_date][2][is_retweet].append(tweet["user_screen_name"])

def time_metrics(tweet, time_set, is_retweet):
	tweet_created_time = datetime.fromisoformat(tweet["created_at"]).strftime("%H") #change to %I %p for AM/PM
  
	if not tweet_created_time in time_set:
		retweet_status = [0,0,0]
		retweet_status[is_retweet] = 1
		retweet_status[not is_retweet] = 0
		time_set[tweet_created_time] = retweet_status
		retweet_status[2] = [0,0]
		retweet_status[2][0] = []
		retweet_status[2][1] = []
	else:
		time_set[tweet_created_time][is_retweet] += 1
	time_set[tweet_created_time][2][is_retweet].append(tweet["user_screen_name"])

def user_metrics(tweet, user_set, is_retweet, keep_rt):
	if ("retweeted_status" in tweet) == False or keep_rt == "True":
		user = {}
		if tweet["user_screen_name"] not in user_set:
			user["screen_name"] = tweet["user_screen_name"]
			user["description"] = tweet["user_description"]
			user["following_count"] = tweet["user_following_count"]
			user["followers_count"] = tweet["user_followers_count"]
			user["total_tweets"] = tweet["user_total_tweets"]
			user["created_at"] = tweet["user_created_at"]
			user["total_in_data_set"] = [0,0]
			user["total_in_data_set"][is_retweet] = 1
			## Note, if full retweet count is preferred, replace "retweet_count_dataset" with "retweet_count_listed"
			if "retweet_count_dataset" in tweet:
				user["total_times_retweeted_in_dataset"] = int(tweet["retweet_count_dataset"])
			user_set[tweet["user_screen_name"]] = user

		else:			
			user_set[tweet["user_screen_name"]]["total_in_data_set"][is_retweet] += 1
			if "retweet_count_dataset" in tweet:
				user_set[tweet["user_screen_name"]]["total_times_retweeted_in_dataset"] += int(tweet["retweet_count_dataset"])
			if tweet["user_following_count"] > user_set[tweet["user_screen_name"]]["following_count"]:
				user_set[tweet["user_screen_name"]]["following_count"] 
			if tweet["user_followers_count"] > user_set[tweet["user_screen_name"]]["followers_count"]:
				user_set[tweet["user_screen_name"]]["followers_count"] 
			if tweet["user_total_tweets"] > user_set[tweet["user_screen_name"]]["total_tweets"]:
				user_set[tweet["user_screen_name"]]["total_tweets"] 

		#if tweet["user_screen_name"] not in unique_users[is_retweet]:
		#	unique_users[is_retweet].append(tweet["user_screen_name"])

def save_hashtag_metrics(hashtags, file_name):
	with open('./results/metrics_%s/%s_hashtags.csv' % (file_name, file_name), 
		mode='w', encoding="utf-8",newline='') as file_hashtags:
		writer_hashtags = csv.writer(file_hashtags)
		writer_hashtags.writerow(["hashtag","total_normal", "total_retweet", 
			"total","unique_tweeters", "re_unique_tweeters", "re_unique_tweeters_filtered", "total"])

		for hashtag, value in hashtags.items():
			normal = set(value[2][0])
			retweet = set(value[2][1])
			unique = [x for x in retweet if x not in normal]
			writer_hashtags.writerow([hashtag, value[0], value[1], value[0] + value[1], str(len(normal)), 
				str(len(retweet)), str(len(unique)), str(len(normal) + len(unique))])
	print("Finished. Saved to ./results/metrics_%s/%s_hashtags.csv" % (file_name, file_name))

def save_date_metrics(date_set, file_name):
	with open('./results/metrics_%s/%s_date.csv' % (file_name, file_name), 
		mode='w', encoding="utf-8",newline='') as file_date:
		writer_date = csv.writer(file_date)
		writer_date.writerow(["date","total_normal", "total_retweet", "total_tweets","unique_normal_tweeters",
			"unique_retweeters_exist", "unique_retweeters_filtered", "unique_retweeters_total", 
			"total_tweeters"])

		for date, value in date_set.items():
			#unique_users[is_retweet]

			normal = set(value[2][0])
			retweet = set(value[2][1])
			unique = [x for x in retweet if x not in normal]
			writer_date.writerow([date, value[0], value[1], value[0] + value[1], 
				str(len(normal)), str(len(retweet) - len(unique)), str(len(unique)), str(len(retweet)), 
				str(len(normal) + len(unique))])

	print("Finished. Saved to ./results/metrics_%s/%s_date.csv" % (file_name, file_name))

def save_time_metrics(time_set, file_name):
	with open('./results/metrics_%s/%s_time.csv' % (file_name, file_name), 
		mode='w', encoding="utf-8",newline='') as file_time:
		writer_time = csv.writer(file_time)
		writer_time.writerow(["time","total_normal", "total_retweet", "total_tweets","unique_tweeters", 
			"re_unique_tweeters", "re_unique_tweeters_filtered", "total_tweeters"])

		for hour, value in time_set.items():
			normal = set(value[2][0])
			retweet = set(value[2][1])
			unique = [x for x in retweet if x not in normal]
			writer_time.writerow([hour, value[0], value[1], value[0] + value[1], 
				str(len(normal)), str(len(retweet)), str(len(unique)), str(len(normal) + len(retweet))])
	print("Finished. Saved to ./results/metrics_%s/%s_time.csv" % (file_name, file_name))

def save_user_metrics(user_set, file_name):
	with open('./results/metrics_%s/%s_users.csv' % (file_name, file_name), 
		mode='w', encoding="utf-8",newline='') as file_users:
		writer_users = csv.writer(file_users)
			

		if "total_times_retweeted_in_dataset" in list(user_set.values())[0]:
			writer_users.writerow(["screen_name", "total_posted_normal","total_posted_retweets","total_posted", 
				"user_description","user_following_count", "user_followers_count", "user_total_tweets", 
				"total_times_retweeted_in_dataset","user_created_at"])
			with open('./results/metrics_%s/%s_old_retweets.csv' % (file_name, file_name), 
				mode='r', encoding="utf-8") as old_retweets:
				csv_reader = csv.DictReader(old_retweets)
				for tweet in csv_reader:
					if tweet["user_screen_name"] in user_set:
						user_set[tweet["user_screen_name"]]["total_times_retweeted_in_dataset"] += int(tweet["retweet_count"])
		else:
			writer_users.writerow(["screen_name", "total_posted_normal","total_posted_retweets","total_posted", 
				"user_description","user_following_count", "user_followers_count", "user_total_tweets","user_created_at"])

		for a in user_set:
			user = user_set[a]
			if "total_times_retweeted_in_dataset" not in user:
				writer_users.writerow([user["screen_name"],user["total_in_data_set"][0],user["total_in_data_set"][1], 
					(user["total_in_data_set"][0] + user["total_in_data_set"][1]), user["description"],
					user["following_count"],user["followers_count"],user["total_tweets"],user["created_at"]])
			else:
				writer_users.writerow([user["screen_name"],user["total_in_data_set"][0],user["total_in_data_set"][1], 
					(user["total_in_data_set"][0] + user["total_in_data_set"][1]), user["description"],
					user["following_count"],user["followers_count"],user["total_tweets"],
					user["total_times_retweeted_in_dataset"],user["created_at"]])

	print("Finished. Saved to ./results/metrics_%s/%s_users.csv" % (file_name, file_name))


if __name__ == '__main__':
	#pass in the target filename without "json" as argument in command prompt.
	parse_tweets(sys.argv)