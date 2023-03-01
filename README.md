# Big-Data-Management-with-Pyspark-RDD
This project is to use Pyspark RDD(resilient distributed dataset) to manage and do analysis on a large scale of Yelp Data, to show my ability to use RDD and my understanding of distributed data

task1: 
You will work on test_review.json, which contains the review information from users, and write a
program to automatically answer the following questions:\n
A. The total number of reviews (0.5 point)\n
B. The number of reviews in 2018 (0.5 point)\n
C. The number of distinct users who wrote reviews (0.5 point)\n
D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
(0.5 point)\n
E. The number of distinct businesses that have been reviewed (0.5 point)\n
F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
(0.5 point)\n
Input format: (we will use the following command to execute your code)\n
Python:\n
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G\n
task1.py <review_filepath> <output_filepath>\n
Scala:\n
spark-submit --class task1 --executor-memory 4G --driver-memory 4G hw1.jar <review_filepath>\n
<output_filepath>\n
Output format:
IMPORTANT: Please strictly follow the output format since your code will be graded automatically.
a. The output for Questions A/B/C/E will be a number. The output for Questions D/F will be a list, which
is sorted by the number of reviews in the descending order. If two user_ids/business_ids have the same
number of reviews, please sort the user_ids /business_ids in the lexicographical order.
b. You need to write the results in the JSON format file. You must use exactly the same tags (see the red
boxes in Figure 2) for answering each question.

![image](https://user-images.githubusercontent.com/43727688/222016738-a56114a1-d85e-45ec-857e-65c9a88643fd.png)




