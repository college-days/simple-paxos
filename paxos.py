# -*- coding=utf-8 -*-

import unicodefilter
from leader import Leader
from proposer import Proposer
from acceptor import Acceptor
from multiprocessing import Queue

if __name__ == '__main__':
	acceptorTotalNum = 20
	proposerTotalNum = 5

	queueToAcceptors = []
	queueToProposers = []

	proposers = []
	acceptors = []

	queueToLeader = Queue()

	for i in range(0, acceptorTotalNum):
		queueToAcceptors.append(Queue())

	for i in range(0, proposerTotalNum):
		queueToProposers.append(Queue())

	leader = Leader("leader", queueToLeader, queueToProposers, acceptorTotalNum)
	#leader.setDaemon(True)
	leader.start()

	for i in range(0, proposerTotalNum):
		proposers.append(Proposer("proposer" + str(i), queueToLeader, queueToAcceptors, queueToProposers[i], i))

	for i in range(0, acceptorTotalNum):
		acceptors.append(Acceptor("acceptor" + str(i), queueToProposers, queueToAcceptors[i], i))

	for i in range(0, len(acceptors)):
		#acceptors[i].setDaemon(True)
		acceptors[i].start()

	for i in range(0, len(proposers)):
		#proposers[i].setDaemon(True)
		proposers[i].start()

