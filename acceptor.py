# -*- coding=utf-8 -*-

import random
import threading
from Queue import Empty
from logger import log

#acceptor接受proposer提交的议案进行决策和仲裁并最终确定一个一致的议案
#currentThreadName -> 当前acceptor线程的名字
#queueFromAcceptorToProposers -> acceptor向proposer发送消息的队列列表 每一个队列和一个proposer进行交互
#queueFromProposersToAcceptor -> proposer向acceptor发送消息的队列
#acceptorid -> 当前acceptor的id标识
class Acceptor(threading.Thread):
	def __init__(self, currentThreadName, queueFromAcceptorToProposers, queueFromProposersToAcceptor, acceptorid):
		threading.Thread.__init__(self, name=currentThreadName)
		self.receiveQueue = queueFromProposersToAcceptor
		self.responseQueues = queueFromAcceptorToProposers
		self.acceptorid = acceptorid
		self.values = {
			"latest_prepared_epoch": 0,
			"accepted_epoch": 0,
			"accepted_value": "empty"
		}

	def run(self):
		while True:
			try:
				proposal = self.receiveQueue.get(True, 10)
				response = self.processProposal(proposal)
				if random.randrange(100) < 98:
					self.responseQueues[proposal["id"]].put(response)
				else:
					log(self.name + " 发送审批失败")
			except Empty:
				continue

	def prepare(self, proposal):
		response = {}

		if proposal["epoch"] >= self.values["latest_prepared_epoch"]:
			self.values["latest_prepared_epoch"] = proposal["epoch"]
			response = {
				"type": "prepare",
				"result": "accept",
				"flag": "ok",
				"accepted_epoch": self.values["accepted_epoch"],
				"accepted_value": self.values["accepted_value"],
				"acceptor": self.acceptorid,
				"time": proposal["time"],
				"id": proposal["id"]
			}

			log("acceptor %s prepare to accept the value -> %s from proposer %s" % (self.acceptorid, proposal["value"], proposal["id"]))

		else:
			response = {
				"type": "prepare",
				"result": "reject",
				"flag": "error",
				"accepted_epoch": self.values["accepted_epoch"],
				"accepted_value": self.values["accepted_value"],
				"acceptor": self.acceptorid,
				"time": proposal["time"],
				"id": proposal["id"]
			}

		return response

	def accept(self, proposal):
		response = {}

		if proposal["epoch"] == self.values["latest_prepared_epoch"]:
			self.values["accepted_epoch"] = proposal["epoch"]
			self.values["accepted_value"] = proposal["value"]
			response = {
				"type": "accept",
				"result": "chosen",
				"flag": "ok",
				"accepted_epoch": self.values["accepted_epoch"],
				"accepted_value": self.values["accepted_value"],
				"acceptor": self.acceptorid,
				"time": proposal["time"],
				"id": proposal["id"]
			}

			log("acceptor %s accept the value -> %s from proposer %s" % (self.acceptorid, proposal["value"], proposal["id"]))

		else:
			response = {
				"type": "accept",
				"result": "reject",
				"flag": "error",
				"accepted_epoch": self.values["accepted_epoch"],
				"accepted_value": self.values["accepted_value"],
				"acceptor": self.acceptorid,
				"time": proposal["time"],
				"id": proposal["id"]
			}

		return response

	def processProposal(self, proposal):
		response = {}

		step = proposal["step"]
		if step == "prepare":
			response = self.prepare(proposal)
		elif step == "accept":
			response = self.accept(proposal)
		else:
			log("some thing is wrong with the proposal step")

		return response