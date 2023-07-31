import os
import sys
import pyspark
from pyspark.streaming import StreamingContext

from collections import namedtuple

import exercise1
import exercise2
import exercise3
	
HADOOP_HOME = "path to HADOOP HOME"
BATCH_DURATION_SECONDS = 1

def create_tweet(data_line):
	tweetsInBatch = []
	tweets = data_line.split("[Tweet]")
	for tweet in tweets:
		tweet_parts = tweet.split("*|*")
		if(len(tweet_parts) == 2):
			tweetsInBatch.append(Tweet(tweet_parts[0],tweet_parts[1]))
		else:
			if(len(tweet_parts) == 1):
				tweetsInBatch.append( Tweet("1", tweet_parts[0]))
			else:
				tweetsInBatch.append( Tweet("",""))
	return tweetsInBatch

if(__name__== "__main__"):
	os.environ["HADOOP_HOME"] = HADOOP_HOME 
	sys.path.append(HADOOP_HOME + "\\bin")
	conf = pyspark.SparkConf().setAppName("SparkStreaming_I")
	sc = pyspark.SparkContext(conf=conf)
	sc.setLogLevel("ERROR")
	ssc = StreamingContext(sc, BATCH_DURATION_SECONDS)
	ssc.checkpoint("./checkpoint")
	socket_stream = ssc.socketTextStream("localhost", 9009)
	received_data = socket_stream.window(BATCH_DURATION_SECONDS)
	fields = ("id", "text" )
	Tweet = namedtuple( 'Tweet', fields )
	tweets = received_data.flatMap(lambda data_line: create_tweet(data_line))
	if(len(sys.argv) < 2):
		print("You have to provide an exercise number (exercise1, exercise2, exercise3)")
		exit()
	else:
		if(sys.argv[1] == "exercise1"):
			exercise1.displayAllTweets(tweets)
		else :
			if(sys.argv[1] == 'exercise2'):
				exercise2.get10MostPopularHashtagsInLast5min(tweets)
			else:
				if(sys.argv[1] == 'exercise3'):
					exercise3.sentimentAnalysis(tweets)
				else:
					print("Wrong exercise number (exercise1, exercise2)")
					exit()
	
	ssc.start()
	ssc.awaitTermination()