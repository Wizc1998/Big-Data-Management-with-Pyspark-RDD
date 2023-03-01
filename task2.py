# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 13:23:33 2023

@author: jason
"""

from pyspark import SparkContext
import os
import sys
import json
import time
import math



os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

review_filepath = "./data/review.json"
output_filepath = 'task2_result.json'

sc = SparkContext("local","task").getOrCreate()
reviews_rdd = sc.textFile(review_filepath).map(lambda x: json.loads(x))
sc.setLogLevel('ERROR')

n_elements = reviews_rdd.count()

#%%


partition_n = 2


#function to display n_items

def f(partitionData):
    cc=0
    for element in partitionData:
        cc+=1
    yield cc


#default_n_partition = reviews_rdd.getNumPartitions()    
#default_n_items = reviews_rdd.mapPartitions(f).collect()


#print('\nrdd default\n',default_n_partition)
#print(default_n_items)


def partitioner(key):
    return ord(key[:1])


#new way
time1 = time.perf_counter()
new_rdd = reviews_rdd.map(lambda x:[x.get('business_id'),1]).partitionBy(partition_n, partitioner).reduceByKey(lambda x,y: x+y)
new_results = new_rdd.takeOrdered(10, key=lambda x: (-1 * x[1], x[0]))
time2 = time.perf_counter()

customize_time = time2-time1

customize_n_partition = new_rdd.getNumPartitions()    
customize_n_items = new_rdd.mapPartitions(f).collect()
print('\nnew way results\n',customize_n_partition)
print(customize_n_items)
print('time: ',customize_time)




#old way
time1 = time.perf_counter()

old_rdd = reviews_rdd.map(lambda x: [x.get('business_id'),1]).reduceByKey(lambda x,y: x+y)

#old_results = old_rdd.map(list).sortBy(lambda x: x[1],ascending=False).take(10)
old_results = old_rdd.takeOrdered(10, key=lambda x: (-1 * x[1], x[0]))

time2 = time.perf_counter()
default_time = time2-time1

default_n_partition = old_rdd.getNumPartitions()    
default_n_items = old_rdd.mapPartitions(f).collect()
print('\nold way results\n',default_n_partition)
print(default_n_items)
print('time: ',default_time)


#%%

new_results == old_results

#output results:

output_dict = {
        "default":{
            "n_partition":default_n_partition,
            "n_items":default_n_items,
            "exe_time":default_time
        },
        "customized":{
            "n_partition":customize_n_partition,
            "n_items":customize_n_items,
            "exe_time":customize_time
        },
    
    }

with open(output_filepath, "w") as outfile:
    json.dump(output_dict, outfile)

outfile.close()











