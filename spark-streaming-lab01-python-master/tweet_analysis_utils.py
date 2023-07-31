from langdetect import detect
import re

class TweetAnalysisUtils:

	def __init__(self):
		stopWordsFile = open("./resources/stop-words.txt","r")
		self.stopWords = stopWordsFile.read().splitlines()
		stopWordsFile.close()
		posWordsFile = open("./resources/pos-words.txt", "r")
		self.posWords = posWordsFile.read().splitlines()
		posWordsFile.close()
		negWordsFile = open("./resources/neg-words.txt", "r")
		self.negWords = negWordsFile.read().splitlines()
		negWordsFile.close()

	def isEnglish(self, tweetText):
		try:
			language = detect(tweetText)
		except:
			language = "unk"
		if language == "en":
			return True
		else:
			return False

	def cleanTweet(self, tweet):
		return
	
	
	def stemTweet(self, tweet):
		return

	def getPositiveScore(self, tweet):
		return

	def getNegativeScore(self, tweet):
		return