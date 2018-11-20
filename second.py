#!/usr/bin/python
# -*- coding :utf-8 -*-
#!/usr/bin/python
# -*- coding :utf-8 -*-
import sys
from pyspark import SparkContext
from pyspark import SparkConf
#读取给定文件内容
f1=open('F:\研一上\云计算\home\homework02\\variables.txt','r')
#存文件内容
array=f1.read().splitlines()
k=int(array[0])
filein=array[1]
fileout=array[2]
f1.close()

#输出文件内容
#print(k,filein,fileout)
#启动spark
conf=SparkConf().setMaster('local').setAppName("second")
sc = SparkContext(conf=conf)
#读数据集
lines = sc.textFile(filein)
#print(lines.collect())
#配对pairs rdd
rdd=lines.map(lambda x:(x.split(',')[0],float(x.split(',')[1])))
rdd_add=rdd.reduceByKey(lambda x, y: x+y)
rdd_sort=rdd_add.sortBy(ascending=False,numPartitions=None,keyfunc=lambda x:x[1])
#返回rdd中所有元素
result=rdd_sort.collect()
i=0
f2=open(fileout+'mf1832107.txt','w')
for(word, count) in  result:
	if i<k:
		f2.write(word)
		f2.write('\n')
		print(word)
		i=i+1
	else:
		break
#关闭spark,和写文件
sc.stop()
f2.close()




