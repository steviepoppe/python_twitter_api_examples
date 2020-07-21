import tweepy
import json
import sys
from datetime import datetime
from pathlib import Path

# This python script accesses the Twitter stream API using the third-party python plug-in tweepy
# When running from the command prompt, it expects at least one argument: the search query
# (optionally, just input the search query in the global variable search_query, can be comma-separated)
# Results are saved as raw JSON objects in a './results/' subdirectory 

# Name of the saved file is based on the search query + part number + current datetime 
# The process can be terminated at any time using ctrl-c
# TO DO: set up a counter and save tweets in chunks (like the search script)


#Twitter API credentials
consumer_key = 'your_consumer_key_here'
consumer_secret = 'your_consumer_secret_here'
access_key = 'your_access_key_here'
access_secret = 'your_access_secret_here'

tweet_count = 0
search_query = ''
timestamp = datetime.today().strftime('%Y%m%d_%H%M%S')
language = '' #Optional: filtering by which language? Japanese? -> 'ja'

class StreamListener(tweepy.StreamListener):

	def on_data(self, data):
		try:
			with open('./results/stream_tweets_%s_%s.json' % (search_query, timestamp), 
				'a', encoding="utf-8") as file:
				global tweet_count 
				status = json.loads(data)
				#make sure the incoming data is tweet JSON, not rate related JSON
				if "created_at" in status:
					#prettifying json by parsing status string as json and then redumping ?? oof		
					file.write((',' if tweet_count > 0 else '') 
						+ json.dumps(status,ensure_ascii=False,sort_keys = True,indent = 4))			
					tweet_count += 1
					if tweet_count % 10 == 0:
						print("Downloaded %d tweets" % tweet_count)
				return True
		except BaseException as e:
			print("Error on_data: %s" % str(e))			
			return True
 
	def on_error(self, status):
		print("Error status on_error: %s" % str(status))
		return True

if __name__ == '__main__':
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_key, access_secret)
	api = tweepy.API(auth, wait_on_rate_limit=True,
		wait_on_rate_limit_notify=True)

	if len(sys.argv) > 1:
		search_query = sys.argv[1]	
	if len(sys.argv) > 2:
		language = sys.argv[2]

	print("Python stream started. Press ctrl-c to disconnect.")

	#create dir results if != exists
	Path("./results/").mkdir(parents=True, exist_ok=True)

	#very hacky way of creating valid JSON but easier on memory		
	with open('./results/stream_tweets_%s_%s.json' % (search_query, timestamp), 'w', encoding="utf-8") as file:
		file.write('{"objects":[') 

	try:
		while True:
			StreamListener = StreamListener()
			stream = tweepy.Stream(auth = api.auth, listener=StreamListener,tweet_mode='extended')
			stream.filter(track=[search_query], languages=[language])
	except KeyboardInterrupt as e:
		stream.disconnect() #disconnect the stream and stop streaming
		print("Stream disconnected. Downloaded %d tweets, Saved to ./results/stream_tweets_%s_%s.json" 
			% (tweet_count, search_query, timestamp))

	with open('./results/stream_tweets_%s_%s.json' % (search_query, timestamp), 'a') as file:
		file.write(']}')
