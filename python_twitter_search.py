import tweepy
import sys
import json
import os
from datetime import datetime
from pathlib import Path

# This python script accesses the Twitter Search API using the third-party python plug-in tweepy
# When running from the command prompt, it expects at least one argument: the search query
# (optionally, just input the search query in the global variable search_query)
# Results are saved as raw JSON objects in a './results/' subdirectory (contains a ton of valuable meta-data)
# In order to keep the filesize of the results manageable (and bypass a tweepy memory leak), 
# objects are saved per 10k (re)tweets at a time
# Names of the saved files are based on the search query + part number + current datetime 
# The process can be terminated at any time using ctrl-c; the last ID will be printed before exit

# Use the last ID as max_id to continue mining tweets tweeted 
# before the max time span collected until process termination

# When continuing the process at a later time (within a timespan of a week), use the first tweet_id
# of the first tweet in the first JSON file as the since_id value to continue mining from that point onwards

#Twitter API credentials
consumer_key = 'your_consumer_key_here'
consumer_secret = 'your_consumer_key_here'
access_key = 'your_consumer_key_here'
access_secret = 'your_consumer_key_here'

max_counter = 10001  #set to 0 to save all tweets to one file instead of chunking in pieces
max_id = None #Optional: Until which ID?
since_id = 1527316759719854080 #Optional: Since which ID?
first_id = None 
tweet_total_count = 0
language = None #Optional: filtering by which language? Japanese? -> 'ja'
search_query = ''
quit = False

def search_tweets(sys_args):

	global max_id
	global since_id
	global first_id
	global search_query
	global language
	global tweet_total_count

	if len(sys_args) > 1:
		search_query = sys_args[1]
	if len(sys_args) > 2:
		max_id = sys_args[2]
	if len(sys_args) > 3:
		since_id = sys_args[3]
	if len(sys_args) > 4:
		language = sys_args[4]

	auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
	api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

	#tweet_total_count = 0
	timestamp = datetime.today().strftime('%Y%m%d_%H%M%S')
	part = 0
	tweet_count = None;

	#create dir results if != exists
	Path("./results/").mkdir(parents=True, exist_ok=True)

	while tweet_count != 0 and quit == False:
		part += 1
		tweet_count = process_tweets(api, search_query, timestamp, part)
		tweet_total_count += tweet_count

	#To do: save last tweet ID in the registry in order to automatize with batch scripts
	print ("Finished process. Downloaded %d total tweets. Last tweet ID was %s and first ID was %s" % (tweet_total_count, max_id, first_id))

def process_tweets(api, search_query, timestamp, part):
	global max_id 
	global first_id
	global tweet_total_count
	global quit 
	with open('./results/search_tweets_%s_%s_part-%s.json' % (search_query, timestamp, part), 
		mode='w', encoding="utf-8") as file:
		#bit of a hacky way to create valid JSON but easier on memory
		file.write('{"objects":[') 
		tweet_count = 0
		try:
			# cursor pagination, 100 is limit of returned tweets per access call
			for status in tweepy.Cursor(api.search,q=search_query, count=100, 
				tweet_mode='extended',
				lang=language,
				since_id=since_id,
				max_id=max_id
				).items(max_counter):
				# tweepy takes max_id as first id to return: already have this so skip 
				# (that's also why the max_counter is 10001 instead of 10k)
				if tweet_total_count == 0 and tweet_count == 0:
					first_id = status.id_str
				if max_id != status.id_str:
					max_id = status.id_str
					#conditional operator is part of the manual JSON parsing hack
					file.write((',' if tweet_count > 0 else '') + json.dumps(status._json,
						ensure_ascii=False,sort_keys = True,indent = 4))
					tweet_count += 1
					if tweet_count % 100 == 0:
						print("Downloaded %d tweets" % tweet_count)	
		except KeyboardInterrupt:
			print("Process terminated. Last tweet ID was %s and first ID was %s" % (max_id, first_id))
			quit = True
		except tweepy.TweepError:
			print("Memory error. Last tweet ID was %s and first ID was %s" % (max_id, first_id))

		file.write(']}')

	#To do: we don't know if we've reached the last tweet until the tweepy API call, which happens after creating
	# a new JSON file. For now, this just removes the empty, final json file
	#optionally we could keep all objects, per chunk of 10k, in memory and save at the end but this is way more memory-taxing
	if tweet_count > 0:
		print ("Downloaded %d tweets, Saved to ./results/search_tweets_%s_%s_part-%s.json" % (tweet_count, search_query, timestamp, part))
	else:
		os.remove("./results/search_tweets_%s_%s_part-%s.json" % (search_query, timestamp, part))

	#no need to loop if all tweets are saved in one file
	if max_counter == 0:
		quit = True

	return tweet_count

if __name__ == '__main__':
	search_tweets(sys.argv)