import sys
import csv
from itertools import chain
from functools import reduce
import re
import os
import pandas as pd
import neologdn
from datetime import datetime

# this script creates a CSV file of the text content of all tweets, using the full CSV generate with python_parse_tweet_ver2.py as input
# it uses several third-party libraries for cleaning the content, such as itertools, functools and neologd
# and, again, pandas to read large CSV files in chunks 

def parse_tweets(sys_args):
	file_name = sys_args[1]
	save_file_name = sys_args[2] if len(sys_args) > 2 else datetime.today().strftime('%Y%m%d_%H%M%S')
	text_column = sys_args[3] if len(sys_args) > 3 else "text"
	chunksize = 100000
	line_count = 0
	tweet_list = list()
	exists = os.path.isfile('./results/%s_tweet_%s.csv' % (file_name,text_column))
	
	for chunk in pd.read_csv('./results/%s.csv' % file_name, encoding="utf-8", chunksize=chunksize, iterator=True, 
		usecols=[text_column, "user_screen_name", "hashtags", "user_mentions", "is_retweet"]):
		for index, tweet in chunk.iterrows():
			line_count += 1
			hashtags =  tweet["hashtags"].split(",") if pd.notna(tweet["hashtags"]) else []
			user_mentions = tweet["user_mentions"].split(",") if pd.notna(tweet["user_mentions"]) else []
			if tweet["is_retweet"] == False:
				tweet_row = {}
				tweet_text = neologdn.normalize(tweet[text_column], repeat=2)
				tweet_text = clean_tweets(tweet_text, hashtags, user_mentions)
				tweet_row["text"] = tweet_text.encode('cp932',"ignore").decode('cp932')
				tweet_row["user_screen_name"] = tweet["user_screen_name"].encode('cp932',"ignore").decode('cp932')
				print(tweet_row["user_screen_name"])
				tweet_list.append(tweet_row)
		print('Processed %s lines.' % line_count)
		
	print('Processed total of %s lines.' % line_count)
	tweet_json = pd.DataFrame(tweet_list)
	with open('./results/%s_tweet_%s.csv' % (save_file_name,text_column), mode='a', encoding="cp932",newline='') as file:
		tweet_json.to_csv(file, header=(not exists), index = False)

def clean_tweets(text, hashtags, user_mentions):
	text = reduce(lambda t, s: t.replace(s, ""), chain(hashtags, user_mentions), text)
	text = re.sub(r'https?://\S+', '', text)	
	text = text.lower()
	text = re.sub('[!"#$%&\'\\\\()*+,-./:;<=>?@[\\]^_`{|}~「」〔〕“”〈〉『』【】＆＊・（）＄＃＠。、？！｀＋￥％]', ' ', text)	
	text = re.sub(r'　', ' ', text)
	return text

if __name__ == '__main__':
	parse_tweets(sys.argv)
