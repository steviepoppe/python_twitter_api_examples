import tweepy
import json
import sys
from datetime import datetime
from pathlib import Path

# This python script accesses the Twitter user timeline API using the third-party python plug-in tweepy
# When running from the command prompt, it expects one argument: the search query
# (optionally, just input the search query in the global variable search_query, can be comma-separated)
# Results are saved as raw JSON objects in a './results/' subdirectory 

# Name of the saved file is based on the search query + current datetime 
# The process can be terminated at any time using ctrl-c

#Twitter API credentials
consumer_key = 'your_consumer_key_here'
consumer_secret = 'your_consumer_secret_here'
access_key = 'your_access_key_here'
access_secret = 'your_access_secret_here'

search_query = ''

def get_timeline_tweets(screen_name):

	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_key, access_secret)
	api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

	#create dir results if != exists
	Path("./results/").mkdir(parents=True, exist_ok=True)

	tweet_count = 0; 
	timestamp = datetime.today().strftime('%Y%m%d_%H%M%S')

	with open('./results/timeline_tweets_%s_%s.json' % (screen_name, timestamp), 
		mode='w', encoding="utf-8") as file:
		#bit of a  hacky way to create valid JSON but easier on memory
		file.write('{"objects":[') 
		try:
			# cursor pagination, 200 is limit of returned Tweets per access call
			for status in tweepy.Cursor(api.user_timeline, screen_name=screen_name, 
				count=200, tweet_mode='extended').items():
				#set ensure_ascii to true to encode Unicode in ascii. 
				#',' conditional operator is part of the manual JSON parsing hack
				file.write((',' if tweet_count > 0 else '') + 
					json.dumps(status._json,ensure_ascii=False,sort_keys = True,indent = 4))
				tweet_count += 1
				if tweet_count % 200 == 0:
					print("Downloaded %d tweets" % tweet_count)	
		except KeyboardInterrupt:
			print("Process terminated.")
						
		file.write(']}')

	print("Downloaded %d tweets, Saved to ./results/timeline_tweets_%s_%s.json" 
		% (tweet_count, screen_name, timestamp))

if __name__ == '__main__':
    #pass in the username of the account you want to download as argument in command prompt. 
    #Feel free to replace 'sys.argv[1]' with the search target (e.g. "name").

	if len(sys.argv) > 1:
		search_query = sys.argv[1]

	get_timeline_tweets(search_query)
