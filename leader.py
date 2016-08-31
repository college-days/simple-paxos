# -*- coding=utf-8 -*-

import random
import threading
from logger import log

#leader用来负责分发议案给proposer来提交给acceptor
#currentThreadName -> 当前leader线程的名字
#queueFromProposerToLeader -> proposer线程通过该队列向leader发送消息
#queueFromLeaderToProposers -> leader向proposer发送消息的队列列表每一个队列和一个proposer进行交互
#acceptorTotalNum -> acceptor的数量
class Leader(threading.Thread):
	def __init__(self, currentThreadName, queueFromProposerToLeader, queueFromLeaderToProposers, acceptorTotalNum):
		threading.Thread.__init__(self, name=currentThreadName)
		self.receiveQueue = queueFromProposerToLeader
		self.responseQueues = queueFromLeaderToProposers
		self.acceptorList = range(0, acceptorTotalNum)
		self.proposalIndex = 0
		self.proposalValues = [
			"<set var -> a>",
			"<set var -> b>",
			"<set var -> c>",
			"<set var -> d>",
			"<set var -> e>",
			"<set var -> f>"
		]
		self.proposalid = 0

	def run(self):
		while True:
			request = self.receiveQueue.get()
			if request["type"] == "request":
				#acceptors = random.sample(self.acceptorList, len(self.acceptorList)/2 + 1)
				#log("proposer %s 要发送决议给 %s 个acceptor" % (request["id"], str(len(acceptors))))

				response = {
					"proposalvalue": self.proposalValues[self.proposalIndex],
					"proposalid": self.proposalid,
					#"acceptors": acceptors
					"acceptors": self.acceptorList
				}

				self.proposalIndex += 1
				#proposalid就是分配给每一个proposer的epoch
				self.proposalid += 1

			self.responseQueues[request["id"]].put(response)
