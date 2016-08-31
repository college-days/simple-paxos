# -*- coding=utf-8 -*-

import time
import random
import threading
from logger import log
from Queue import Empty

#proposer用来从leader中获取议案并且提交给acceptor来进行仲裁和决议
#currentThreadName -> 当前proposer线程名字
#queueFromProposerToLeader -> proposer和leader发送消息的队列
#queueFromProposerToAcceptors -> proposer和acceptor发送消息的队列列表每一个队列和一个acceptor进行交互
#queueFromLeaderOrAcceptorsToProposer -> 所有acceptor和leader向该proposer发送消息的队列
#proposerid -> 当前proposal的id
class Proposer(threading.Thread):
	def __init__(self, currentThreadName, queueFromProposerToLeader, queueFromProposerToAcceptors, queueFromLeaderOrAcceptorsToProposer, proposerid):
		threading.Thread.__init__(self, name=currentThreadName)	
		self.requestQueue = queueFromProposerToLeader
		self.receiveQueue = queueFromLeaderOrAcceptorsToProposer
		self.responseQueues = queueFromProposerToAcceptors
		self.proposerid = proposerid
		self.acceptcount = 0
		self.chosencount = 0
		self.rejectcount = 0
		self.startPropose = False
		self.startTime = 0
		self.messageList = []
		self.failList = []

	def run(self):
		self.requestValueFromLeader()
		log("proposer%d 从leader中获得议案: %s 要向 %s 个acceptor发送议案" % (self.proposerid, str(self.proposalvalue), str(len(self.acceptors))))

		startSignal = {
			"type": "start",
			"id": self.proposerid,
			"time": 0
		}
		self.receiveQueue.put(startSignal)

		while True:
			try:
				message = self.receiveQueue.get(True, 10)
				self.processMessage(message)
			except Empty:
				log("len of proposer%d messageList -> %d" % (self.proposerid, len(self.messageList)))
				self.communicateWithAcceptor()

	def stop(self):
		self.stopped = True

	def requestValueFromLeader(self):
		request = {
			"type": "request",
			"id": self.proposerid
		}

		self.requestQueue.put(request)
		response = self.receiveQueue.get(True, 10)
		self.epoch = response["proposalid"]
		self.proposalvalue = response["proposalvalue"]
		self.acceptors = response["acceptors"]

	def communicateWithAcceptor(self):
		#typelist = [message["type"] for message in self.messageList]
		#print typelist
		if len(self.messageList) > 0:
			if self.messageList[0]["type"] == "prepare":
				prepareList = filter(lambda message: message["result"] == "accept", self.messageList)
				preparelen = len(prepareList)
				log("proposer%d get %d accepted" % (self.proposerid, preparelen))

				if preparelen > len(self.acceptors) / 2:
					log("proposer%d 获得了提交自己取值的权限" % self.proposerid)
					#得到accepted_value不为empty的最大的epoch对应的accepted_value
					acceptedList = filter(lambda message: message["accepted_value"] != "empty", self.messageList)

					log("proposer%d get acceptedList length is -> %d" % (self.proposerid, len(acceptedList)))

					if len(acceptedList) > 0:
						acceptedMap = dict((message["accepted_value"], message["accepted_epoch"]) for message in acceptedList)
						maxAcceptedEpochValue = max(acceptedMap.iteritems(), key=operator.itemgetter(1))[0]
						self.sendProposal("accept", maxAcceptedEpochValue)
					else:
						self.sendProposal("accept", self.proposalvalue)

				self.messageList = []

			elif self.messageList[0]["type"] == "accept":
				acceptlen = len(filter(lambda message: message["result"] == "chosen", self.messageList))
				log("proposer%d get %d chosen" % (self.proposerid, acceptlen))

				if acceptlen > len(self.acceptors) / 2:
					log("proposer%d 提交的取值 %s 被认可了" % (self.proposerid, self.proposalvalue))

				self.messageList = []

			else:
				pass
		else:
			pass

	def processMessage(self, message):
		log("message type -> " + message["type"])
		log("proposer%d 收到发给 proposer%d 的消息" % (self.proposerid, message["id"]))

		if message["type"] == "start":
			self.sendProposal("prepare", self.proposalvalue)

		if message["time"] == self.startTime:
			self.messageList.append(message)
		else:
			log("proposer%d 收到的消息已经过期" % self.proposerid)

	def sendProposal(self, step, value):
		self.startTime = time.time()
		self.startPropose = True
		time.sleep(1/random.randrange(1, 20))
		log("proposer%d 发送了议案 议案内容为 -> %s" % (self.proposerid, str(value)))
		for acceptor in self.acceptors:
			if random.randrange(100) < 98:
				self.response = {
					"step": step,
					"type": "proposing",
					"epoch": self.epoch,
					"value": value,
					"id": self.proposerid,
					"time": self.startTime
				}
				self.responseQueues[acceptor].put(self.response)
			else:
				log("proposer%d 发送议案失败" % self.proposerid)

			time.sleep(1/random.randrange(1, 10))

	def sendProposalToSingleAcceptor(self, step, value, acceptorid):
		self.startTime = time.time()
		self.startPropose = True
		time.sleep(1/random.randrange(1, 20))
		log("proposer%d 发送了议案 议案内容为 -> %s" % (self.proposerid, str(value)))

		if random.randrange(100) < 98:
			self.response = {
				"step": step,
				"type": "proposing",
				"epoch": self.epoch,
				"value": value,
				"id": self.proposerid,
				"time": self.startTime
			}
			self.responseQueues[acceptorid].put(self.response)
		else:
			log("proposer%d 发送议案失败" % self.proposerid)

		time.sleep(1/random.randrange(1, 10))