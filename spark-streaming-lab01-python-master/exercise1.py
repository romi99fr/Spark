import pyspark

def processRDD(rdd):
	rdd.foreach(lambda rdd: print(rdd[1]))

def displayAllTweets(statuses):
	statuses.foreachRDD(lambda rdd: processRDD(rdd))