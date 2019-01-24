#!/usr/bin/env python
#-*-coding=utf-8-*-

import sys
import re
import time
import os
from pyspark import SparkContext, SparkConf

def myFunc(s):
    #words = s.split(" ")
    words = re.split(" |;|,|.|\t|\"|\'|\n",s)
    return len(words)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: pysparkdemo-offical.py <app-name> <inut-file>"
        exit(-1)

    try:
        sc.stop()
    except:
        pass

    time_now=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))

    #conf = SparkConf().setAppName(sys.argv[1]).setMaster(sys.argv[2])
    conf = SparkConf().setAppName(sys.argv[1])
    sc = SparkContext(conf=conf)
    sgRDD = sc.textFile(sys.argv[2])
    #print "sgRDD is: %s"%(sgRDD.collect())
    sgRDDmap=sgRDD.map(myFunc)
    #print "sgRDDmap is: %s"%sgRDDmap.collect()
    # sgRDDmap.persist()
    total_words_number=sgRDDmap.reduce(lambda a, b: a + b)
    total_number=int(total_words_number)

    print "#####################################################################################################"
    print "#####################################################################################################"
    print "#####################################################################################################"
    print "#####################################################################################################"
    print "this total word is: total_number=%d"%total_number
    print "#####################################################################################################"
    print "#####################################################################################################"
    print "#####################################################################################################"
    print "#####################################################################################################"

    #cmd_insertinto_table="/bin/echo \"insert into pyspark_count values('%s',%d);\"|mysql -h 10.141.27.147 -P 3306 -u root -pmetadata@Tbds.com mikealtrigger"%(time_now,total_number)
    #try:
    #    flag=os.system(cmd_insertinto_table)
    #except:
    #    ErrorInfo = r'import to database error!'
    #    print ErrorInfo

    sc.stop()