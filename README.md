# Big-Data-Management-with-Pyspark-RDD
This project is to use Pyspark RDD(resilient distributed dataset) to manage and do analysis on a large scale of Yelp Data, to show my ability to use RDD and my understanding of distributed data  

task1:   
You will work on test_review.json, which contains the review information from users, and write a  
program to automatically answer the following questions:  
A. The total number of reviews  
B. The number of reviews in 2018 
C. The number of distinct users who wrote reviews 
D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
  
E. The number of distinct businesses that have been reviewed 
F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had  

Input format: (we will use the following command to execute your code)  
Python:  
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G  
task1.py <review_filepath> <output_filepath>  

Output format:  
IMPORTANT: Please strictly follow the output format since your code will be graded automatically.
a. The output for Questions A/B/C/E will be a number. The output for Questions D/F will be a list, which
is sorted by the number of reviews in the descending order. If two user_ids/business_ids have the same
number of reviews, please sort the user_ids /business_ids in the lexicographical order.
b. You need to write the results in the JSON format file. You must use exactly the same tags (see the red
boxes in Figure 2) for answering each question.  

![image](https://user-images.githubusercontent.com/43727688/222016738-a56114a1-d85e-45ec-857e-65c9a88643fd.png)




Task2: Partition (2 points)  
Since processing large volumes of data requires performance optimizations, properly partitioning the  
data for processing is imperative.  
In this task, you will show the number of partitions for the RDD used for Task 1 Question F and the  
number of items per partition.  
Then you need to use a customized partition function to improve the performance of map and reduce  
tasks. A time duration (for executing Task 1 Question F) comparison between the default partition and  
the customized partition (RDD built using the partition function) should also be shown in your results.  


Input format: (we will use the following command to execute your code)  
Python:  
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G  
task2.py <review_filepath> <output_filepath> <n_partition>  













