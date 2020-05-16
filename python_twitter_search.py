import tweepy
import sys
import json
from datetime import datetime
from pathlib import Path

#Twitter API credentials
consumer_key = 'your_consumer_key_here'
consumer_secret = 'your_consumer_secret_here'

def search_tweets(search_query):

	auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
	api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

	tweet_count = 0
	timestamp = datetime.today().strftime('%Y%m%d_%H%M%S')
	
        Path("./results/").mkdir(parents=True, exist_ok=True)
	
	with open('./results/search_tweets_%s_%s.json' % (search_query, timestamp), mode='w', encoding="utf-8") as file:
		#bit of a  hacky way to create valid JSON but easier on memory
		file.write('{"objects":[') 
		try:
			# cursor pagination, 100 is limit of returned Tweets per access call
			for status in tweepy.Cursor(api.search,q=search_query, count=100, tweet_mode='extended').items():
				#set ensure_ascii to true to encode Unicode in ascii. ',' conditional operator is part of the manual JSON parsing hack
				file.write((',' if tweet_count > 0 else '') +json.dumps(status._json,ensure_ascii=False,sort_keys = True,indent = 4))
				tweet_count += 1
				if tweet_count % 100 == 0:
					print("Downloaded %d tweets" % tweet_count)	
		except KeyboardInterrupt:
			print("Process terminated.")
						
		file.write(']}')

	print ("Downloaded %d tweets, Saved to ./results/search_tweets_%s_%s.json" % (tweet_count, search_query, timestamp))

if __name__ == '__main__':
	#pass in the keyword to search tweets	
	search_tweets(sys.argv[1])
	
