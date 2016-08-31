import threading

threadLock = threading.Lock()

filepath = "./log"

def log(message):
	threadLock.acquire()
	print message
	fileHandler = open(filepath, 'a')
	fileHandler.write(message + "\n")
	fileHandler.close()
	threadLock.release()