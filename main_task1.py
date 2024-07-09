from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

def checkLine(line):
    return len(line) == 17

if __name__ == "__main__":  
    if len(sys.argv) != 3:
        print("Usage: task1 <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="PythonTask1")
    lines = sc.textFile(sys.argv[1], 1)

    processed = lines.map(lambda x: x.split(',')) \
        .filter(checkLine) \
        .map(lambda x: (x[0], set(x[1]))) \
        .reduceByKey(lambda x,y: x.union(y)) \
        .mapValues(lambda values: len(values))
        
    topTaxis = processed.top(10, lambda x: x[1])
    
    dataToASingleFile = sc.parallelize(topTaxis).coalesce(1)
    dataToASingleFile.saveAsTextFile(sys.argv[2])
    
    sc.stop()