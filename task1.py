# -*- coding: utf-8 -*-
"""
Created on Mon Feb  6 18:08:41 2023

@author: jason
"""

from pyspark import SparkContext
import os
import sys
import json



os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print(sys.argv)
#review_filepath = sys.argv[1]
#output_filepath = sys.argv[2]

review_filepath = "./data/test_review_ext.json"
output_filepath = 'task1_result.json'



sc = SparkContext("local","task").getOrCreate()


reviews_rdd = sc.textFile(review_filepath).map(lambda x: json.loads(x))
sc.setLogLevel('WARN')
#print everything 
#reviews_rdd.collect()

#test
#get a random entry
#rand = reviews_rdd.collect()[0]
#rand.get('user_id')

#A. The total number of reviews (0.5 point)
#
n_review = reviews_rdd.count()
#print(n_review)

#B. The number of reviews in 2018 (0.5 point)
#reviews_rdd.collect()[0]

n_review_2018 = reviews_rdd.map(lambda x: x.get('date').startswith('2018')).filter(lambda x: x == True).count()


#C. The number of distinct users who wrote reviews (0.5 point)

n_user = reviews_rdd.map(lambda x: x.get('user_id')).distinct().count()


#D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote (0.5 point)


top10_user = reviews_rdd.map(lambda x: [x.get('user_id'),1]).reduceByKey(lambda x,y: x+y).map(list).takeOrdered(10, key=lambda x: (-1 * x[1], x[0]))


#E. The number of distinct businesses that have been reviewed (0.5 point)

n_business = reviews_rdd.map(lambda x: x.get('business_id')).distinct().count()


#F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had(0.5 point)


top10_business = reviews_rdd.map(lambda x: [x.get('business_id'),1]).reduceByKey(lambda x,y: x+y).map(list).takeOrdered(10, key=lambda x: (-1 * x[1], x[0]))



print(n_review)
print(n_review_2018)
print(n_user)
print(top10_user)
print(n_business)
print(top10_business)


q1_result_dict = {'n_review':n_review,
                  'n_review_2018':n_review_2018,
                  'n_user':n_user,
                  'top10_user':top10_user,
                  'n_business':n_business,
                  'top10_business':top10_business}

with open(output_filepath, "w") as outfile:
    json.dump(q1_result_dict, outfile)

outfile.close()



























