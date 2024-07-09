from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

def checkline(line):
    if len(line) != 17:
        return False
    try:
        if convertToHour(line[2]) >= 23:
            return False
        if float(line[5]) <= 0:
            return False
        if float(line[12]) <= 0:
            return False
    except:
        return False    
    
    return True

def convertToHour(entry):
    return int(entry.split(' ')[1].split(':')[0])

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    
    processed = lines.map(lambda x : x.split(',')) \
    .filter(checkline) \
    .map(lambda x: (convertToHour(x[2]), (float(x[12]), float(x[5])))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda value: value[0]/value[1])

    topHours = processed.top(3, lambda x: x[1])
    dataToASingleFile = sc.parallelize(topHours).coalesce(1)
    dataToASingleFile.saveAsTextFile(sys.argv[2])
    
    sc.stop()
