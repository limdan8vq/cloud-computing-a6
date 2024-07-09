from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

def checkLine(line):
    if len(line) != 17:
        return False
    
    try:
        if (float(line[4]) <= 0):
            return False
        
        if (float(line[16]) <= 0):
            return False
    except:
        return False
   
    return True
    
# def combineList(list1, list2) :
#     sum1 = list1[0] + list2[0]
#     sum2 = list1[1] + list2[1]
#     return (sum1, sum2)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: task2 <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="PythonTask2")
    lines = sc.textFile(sys.argv[1], 1)

    processed = lines.map(lambda x: x.split(',')) \
        .filter(checkLine) \
        .map(lambda x: (x[1], (float(x[16]) / (float(x[4]) / 60.0), 1))) \
        .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda value: value[0]/value[1])
    
    topDrivers = processed.top(100, lambda x: x[1])
    dataToASingleFile = sc.parallelize(topDrivers).coalesce(1)
    dataToASingleFile.saveAsTextFile(sys.argv[2])

    #earnings = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
    
    #counts.saveAsTextFile(sys.argv[2])
    
    #output = counts.collect()
    
    sc.stop()
