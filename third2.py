#!/usr/bin/python
# -*- coding :utf-8 -*-
#!/usr/bin/python
# -*- coding :utf-8 -*-
from operator import add
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
import os


f1=open('F:\研一上\云计算\第三次小作业\\variables.txt','r')
#存文件内容
array=f1.read().splitlines()
streaming=array[3]
resultaddress=array[4]
f1.close()

# f=open(resultaddress + "mf1832107.txt", 'a+', encoding='utf-8')
province=["北京市","天津市","上海市","重庆市","河北省","山西省","辽宁省","吉林省","黑龙江省","江苏省","浙江省","安徽省","福建省","江西省","山东省","河南省","湖北省","湖南省","广东省","海南省","四川省","贵州省","云南省","陕西省","甘肃省","青海省","台湾省","内蒙古自治区","广西壮族自治区","西藏自治区","宁夏回族自治区","新疆维吾尔自治区","香港特别行政区","澳门特别行政区"]
def filter(s):
	for sheng in province:
		if sheng in s:
			return sheng
	return None


def updateFunction(newValues, runningCount):
	if runningCount is None:
		runningCount = 0
	return sum(newValues, runningCount)

# def checkShutdownMarker(stopFlag):
#      if stopFlag == False:
#          stopFlag = os.path.exists('F:\software\stop-spark')
#          return stopFlag
def checkShutdownMarker():
	stopFlag = os.path.exists(streaming+'\stop-spark')
	return stopFlag

def savefile(x):

	if str(x[0])!='None':
		with open(resultaddress + "mf1832107.txt", 'a+', encoding='utf-8') as f:
			f.write(str(x[0]))
			f.write('_')
			f.write(str(x[1]))
			f.write(';')
			f.write('\n')
		return
	else:
		return

# def sendPartition(iter):
#     # ConnectionPool is a static, lazily initialized pool of connections
#     connection = ConnectionPool.getConnection()
#     for record in iter:
#         connection.send(record)
#     # return to the pool for future reuse
#     ConnectionPool.returnConnection(connection)

def sendPartition(iter):
	f2=open(resultaddress + "mf1832107.txt", 'a+', encoding='utf-8')
	for record in iter:
		if record[0]!=None:
			f2.write(str(record[0]))
			f2.write('_')
			f2.write(str(record[1]))
			f2.write(';')
	f2.write('\n')
	f2.close()

def savenull():
	with open(resultaddress + "mf1832107.txt", 'a+', encoding='utf-8') as f:
		f.write('\n')


if __name__ == "__main__":
	sparkmaster='local[2]'
	conf = SparkConf().setMaster(sparkmaster).setAppName("third")
	sc = SparkContext(conf=conf)
	ssc = StreamingContext(sc, 10)
	lines = ssc.textFileStream(streaming)
	words = lines.map(lambda x: (filter(x), 1))
	wordCounts = words.reduceByKey(add)
	wordCounts.pprint()
	wordCounts.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
	# wordSave=wordCounts.map(lambda x:savefile(x))
	# wordSave.pprint()
	# savenull()
	# with open(resultaddress + "mf1832107.txt", 'a+', encoding='utf-8') as f:
	# 	f.write('\n')
	runningCounts = words.updateStateByKey(updateFunction)
	runningCounts.pprint()
	runningCounts.foreachRDD(lambda rdd:rdd.foreachPartition(sendPartition))
	# runningSave=runningCounts.map(lambda x:savefile(x))
	# runningSave.pprint()
	# savenull()
	# with open(resultaddress + "mf1832107.txt", 'a+', encoding='utf-8') as f:
	# 	f.write('\n')
	ssc.checkpoint('/spark/ssc')
	ssc.start()
	# ssc.awaitTermination()


    # ssc.awaitTerminationOrTimeout(100)
    # ssc.stop()
    # ssc.awaitTermination()
    # ssc.stop()
    # ssc.awaitTermination()



	checkIntervalMillis = 10
	isStopped = False
	while (isStopped == False):
		print("calling awaitTerminationOrTimeout")

		isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
		if isStopped == True:
			print("confirmed! The streaming context is stopped. Exiting application...")
		else:
			print("Streaming App is still running. Timeout...")


		stopFlag = checkShutdownMarker()
		print (stopFlag)
		if isStopped == False and stopFlag == True:
			print("stopping ssc right now")
			# 第一个true：停止相关的SparkContext。无论这个流媒体上下文是否已经启动，底层的SparkContext都将被停止。
			# 第二个true：则通过等待所有接收到的数据的处理完成，从而优雅地停止。
			ssc.stop(True,True)
			print("ssc is stopped!!!!!!!")
			# runningCounts.pprint()
			# runningSave=runningCounts.map(lambda x:savefile(x))
			# runningSave.pprint()


