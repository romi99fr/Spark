import csv
import time
import random
import datetime
import socket

def process_twitter_stream():
	#populate corpus
	lines = []
	with open('full-corpus.csv', 'r') as fd:
		reader = csv.reader(fd)
		lines = []
		for row in reader:
			lines.append(row)

	#create socket
	TCP_IP = "localhost"
	TCP_PORT = 9009
	conn = None
	socket.setdefaulttimeout(60)
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.bind((TCP_IP, TCP_PORT))
		s.listen(5)
		print("Waiting for TCP connection...")
		conn, addr = s.accept()
		while True:
			randomChoice = random.choice(lines)
			message_text = ("[Tweet]"+str(randomChoice[2]) + "*|*" + randomChoice[4]).replace("\n","")+"\n"
			conn.send(message_text.encode('utf-8'))
			time.sleep(1)
	except socket.error as e:
		print("Error socket: " + str(e))
		return False
	except Exception as e:
		print("General Error: " + str(e))
		return False

if(__name__== "__main__"):
	process_twitter_stream()
