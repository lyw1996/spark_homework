#!/usr/bin/python
# -*- coding :utf-8 -*-
#!/usr/bin/python
# -*- coding :utf-8 -*-
import sys
from decimal import *
import time
#防止递归次数过多
sys.setrecursionlimit(10 ** 8)

#大数比较
def decimalcomparesmall(a,b):
	decimala=Decimal(a)
	decimalb=Decimal(b)
	if decimala<=decimalb:
		return 1

def decimalcomparebig(a,b):
	decimala=Decimal(a)
	decimalb=Decimal(b)
	if decimala>=decimalb:
		return 1


#快排
def QuickSort(array,low,high):
	if(low<high):
		pivot=partition(array,low,high)
		QuickSort(array,low,pivot-1)
		QuickSort(array,pivot+1,high)

def partition(array,low,high):
	pivot2=array[low]
	while(low<high):
		while(low<high and array[high]<=pivot2):
			high=high-1
		array[low]=array[high]
		while(low<high and array[low]>=pivot2):
			low=low+1
		array[high]=array[low]
	array[low]=pivot2
	return low


string_lyw=sys.argv[1]
file=open(string_lyw,'r')
arrayall_lyw=file.read().splitlines()
arraynew_lyw=[Decimal(i) for i in arrayall_lyw]

length=len(arraynew_lyw)
QuickSort(arraynew_lyw,1,length-1)

k=int(arraynew_lyw[0])
for i in range(1,k+1):
	#sys.stdout.write(str(arraynew_lyw[i]))
	print(arraynew_lyw[i],end='')
	if i!=k:
		#sys.stdout.write(',')
		print(',',end='')

