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
        if float(line[4]) <= 0:
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


def spreadAcrossHours(line):
    start_time = line[2].split(' ')[1].split(':')
    hour = int(start_time[0])
    seconds = 60 * int(start_time[1]) + int(start_time[2])
    duration = int(line[4])
    time = duration

    surcharge = float(line[12])
    distance = float(line[5])

    ret = []
    # handle starting hour
    if seconds + time >= 3600:
        prop = (3600 - seconds) / duration
        ret.append((hour, (surcharge * prop, distance * prop)))
        hour = (hour + 1) % 24
        time = seconds + time - 3600

    # handle intermediate hours
    prop = 3600 / duration
    while time >= 3600:
        ret.append((hour, (surcharge * prop, distance * prop)))
        hour = (hour + 1) % 24
        time -= 3600

    # handle final hour
    prop = time / duration
    ret.append((hour, (surcharge * prop, distance * prop)))

    return ret


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="PythonWordCount")
    sc.setLogLevel("DEBUG")

    lines = sc.textFile(sys.argv[1], 1)
    
    processed = lines.map(lambda x : x.split(',')) \
        .filter(checkline) \
        .flatMap(lambda x: spreadAcrossHours(x)) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda value: value[0]/value[1])

    topHours = processed.top(24, lambda x: x[1])
    dataToASingleFile = sc.parallelize(topHours).coalesce(1)
    dataToASingleFile.saveAsTextFile(sys.argv[2])
    
    sc.stop()
