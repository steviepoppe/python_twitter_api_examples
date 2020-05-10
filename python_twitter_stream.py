import tweepy
import csv
import json
import sys
from datetime import datetime

#Twitter API credentials
consumer_key = 'your_consumer_key_here'
consumer_secret = 'your_consumer_secret_here'
access_key = 'your_access_key_here'
access_secret = 'your_access_secret_here'

tweet_count = 0;
search_query = "";
timestamp = datetime.today().strftime('%Y%m%d_%H%M%S')

class StreamListener(tweepy.StreamListener):

	def on_data(self, data):
		try:
			with open('stream_tweets_%s_%s.json' % (search_query, timestamp), 'a', encoding="utf-8") as file:
				global tweet_count 
				status = json.loads(data)
				#make sure the incoming data is actually a tweet, occasional rate related JSON gets returned at times
				if "created_at" in status:
					#prettifying json by parsing status string as json and then redumping ?? oof		
					file.write((',' if tweet_count > 0 else '') + json.dumps(status,ensure_ascii=False,sort_keys = True,indent = 4))			
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
	api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
	search_query = sys.argv[1]
	print("Python stream started. Press ctrl-c to disconnect.")

	#very hacky way of creating valid JSON but easier on memory		
	with open('stream_tweets_%s_%s.json' % (search_query, timestamp), 'w', encoding="utf-8") as file:
		file.write('{"objects":[') 

	#This line filter Twitter Streams to capture data by the command prompt input
	try:
		while True:
			StreamListener = StreamListener()
			stream = tweepy.Stream(auth = api.auth, listener=StreamListener,tweet_mode='extended')
			stream.filter(track=[search_query])
	except KeyboardInterrupt as e:
		stream.disconnect() #disconnect the stream and stop streaming
		print("Stream disconnected. Downloaded %d tweets, Saved to stream_tweets_%s_%s.json" % (tweet_count, search_query, timestamp))

	with open('stream_tweets_%s_%s.json' % (search_query, timestamp), 'a') as file:
		file.write(']}')