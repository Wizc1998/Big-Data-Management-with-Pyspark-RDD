# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 18:24:42 2023

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


review_filepath = "./data/test_review.json"
biz_filepath = "./data/business.json"
output_filepath_question_a = 'task3a_result.txt'
output_filepath_question_b = 'task3b_result.json'


sc = SparkContext("local","task").getOrCreate()
sc.setLogLevel('ERROR')

time_start_loading = time.perf_counter()

reviews_rdd = sc.textFile(review_filepath).map(lambda x: json.loads(x))
biz_rdd = sc.textFile(biz_filepath).map(lambda x: json.loads(x))

#A

review_zip = reviews_rdd.map(lambda x: (x.get('business_id'), x.get('stars')))
business_zip = biz_rdd.map(lambda x: (x.get('business_id'), x.get('city')))
join_zip_unformat = review_zip.join(business_zip).filter(lambda x: x[1][1] is not None)
join_zip_unformat.collect()
join_zip = join_zip_unformat.map(lambda x: (x[1][1],int(x[1][0])))
join_zip.collect()
"""
[('Scottsdale', 1.0),
...
 ('Chandler', 5.0)]
"""
zero_tuple = (0,0) 
rdd_aggre_count_sum = join_zip.aggregateByKey(zero_tuple, lambda sum_cnt,next_val: (sum_cnt[0] + next_val, sum_cnt[1] + 1),
                             lambda sum_cnt,next_partition: (sum_cnt[0] + next_partition[0], sum_cnt[1] + next_partition[1]))
"""
[('Scottsdale', (3, 3)),
...
 ('Chandler', (10, 2))]
"""
city_avg_rdd = rdd_aggre_count_sum.mapValues(lambda count_sum: count_sum[0]/count_sum[1])
city_avg_rdd.collect()

time_before_sorting = time.perf_counter()
loading_time = time_before_sorting-time_start_loading

result_A = city_avg_rdd.takeOrdered(10000000,key=lambda x: (-1 * x[1], x[0]))

print(result_A)


#B

#rdd approach

time_rdd_start = time.perf_counter()

rdd_top_10 = city_avg_rdd.takeOrdered(10,key=lambda x: (-1 * x[1], x[0]))

#print(result_A)
time_rdd_end = time.perf_counter()

rdd_time = time_rdd_end-time_rdd_start


#python approach

time_python_start = time.perf_counter()

city_avg_list = city_avg_rdd.collect()
list_top_10 = sorted(city_avg_list, key = lambda x: (-1 * x[1], x[0]))[:10]

time_python_end = time.perf_counter()

python_time = time_python_end-time_python_start



print('rdd time: ',rdd_time)

print('python time: ',python_time)


reason="""under the condition of test_review(size30) joining with business, the python sort time is 2.256633299999976, while rdd time is 2.2730609000000186. the reason that Python is taking a little bit less time, is because the RDD has to communicate with Spark cluster manager, in order to access some part of data. While the Python list approach is completely on Memory. However, I do believe that as data scale gets larger, the RDD will definitely surpass Python native runtime."""


#write to file
#result_A
write_a = open(output_filepath_question_a, "w")

write_a.write('city,stars\n')

for pair in result_A:
    write_a.write(pair[0]+','+str(pair[1])+'\n')
    
write_a.close()

#result B
#m1 for python
#m2 for spark

result_B_dict = {
    "m1":python_time,
    "m2":rdd_time,
    "reason": reason
    }

with open(output_filepath_question_b, "w") as outfile:
    json.dump(result_B_dict, outfile)

outfile.close()



















