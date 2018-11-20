#!/usr/bin/python
# -*- coding :utf-8 -*-
import time
import pymongo
import sys
import io
import os
import shutil

if os.path.exists("F:\研一上\云计算\home\spark\streaming\\tmp")==False:
	os.makedirs("F:\研一上\云计算\home\spark\streaming\\tmp")

f1=open('F:\研一上\云计算\第三次小作业\\variables.txt','r')
#存文件内容
array=f1.read().splitlines()
mongo_url=array[0]
DATABASE=array[1]
COLLECTION=array[2]
streaming=array[3]
resultaddress=array[4]
f1.close()


if __name__ == '__main__':
	# mongo_url = "localhost:27017"
	client = pymongo.MongoClient(mongo_url)
	# DATABASE = "divorceCase"
	db = client.get_database(DATABASE)
	# COLLECTION = "lawcase"
	db_coll = db.get_collection(COLLECTION)
	cursor = db_coll.find()
	# 只查询head
	query = db_coll.find({}, {"head": 1, '_id': 0})
	# print(list(query))
	a = list(query)
	i = 0
	for line in a:
		if i % 50 == 0:
			now = time.time()
			print(now)
			f1 = open(streaming+"\\tmp\head" + str(now) + '.log', 'a', encoding='utf-8')
			result = line['head']
			result2 = result['text'].strip()
			print(result2)
			f1.write(result2 + '\n')
			i = i + 1
			print(i)
		else:
			result = line['head']
			result2 = result['text'].strip()
			f1.write(result2 + '\n')
			print (result2)
			i = i + 1
			if i % 50 == 0:
				# print (result2)
				f1.close()
				shutil.move(streaming+"\\tmp\head" + str(now) + '.log',streaming)
				time.sleep(1)


	time.sleep(2)
	if os.path.exists(streaming+'\stop-spark')==False:
		os.makedirs(streaming+'\stop-spark')
