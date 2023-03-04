"""
Top 15 Video Games on Amazon by Review Count per Day
Python Version: 3.10.9
Packages: sys, pyspark-3.2.1

Tested using SoC Compute Cluster.
Command used to execute script through Slurm:

srun spark-submit Lab1.py <review json> <metadata json> output

"""

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()


# Slurm Run arguments
# Read Amazon Reviews file and Metadata file
review_file = sys.argv[1] #1st argument
meta_file = sys.argv[2] #2nd argument

# Output file as 3rd argument
output = sys.argv[3]

'''Step 1'''
# Reads reviews json file, extracts necessary fields, adds counter for review count, then saves them to an RDD 
reviews_rdd =  spark.read.json(review_file).rdd
asin_time_rdd = reviews_rdd.map(lambda x: ((x['asin'], x['reviewTime']), x['overall']))\
    .mapValues(lambda x: (x, 1))

# Sums all the ratings and counts the number of reviews per day, then calculates the average rating per day
# Rearrange each tuple to be in the format ((asin, reviewTime), (# of reviews, average rating))
reviews_agg_rdd = asin_time_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
    .mapValues(lambda a: (a[1], a[0]/a[1]))

'''Step 2'''
# Reads the metadata file, extracts the necessary fields, then saves them to an RDD
meta_rdd = spark.read.json(meta_file).rdd
brand_rdd = meta_rdd.map(lambda x: (x['asin'], x['brand']))

'''Step 3'''
# Rearranges each tuple in the review_agg_rdd for join with metadata RDD
review_join = reviews_agg_rdd.map(lambda x: (x[0][0],(x[0][1],x[1])))

# Join reviews RDD with 'brand' field in the metadata RDD. 
# Removes tuples with empty 'brand' field
rdd_tojoin = review_join.join(brand_rdd).filter(lambda x: x[1][1]!= None)

'''Steps 4-5'''
# Sorts rdd_tojoin by the number of reviews per date
# To break ties, it then sorts by average rating
sorted_rdd = rdd_tojoin.sortBy(lambda x: (-x[1][0][1][0],-x[1][0][1][1]))
    
# Rearranges each tuple in the sorted_rdd to the required output format
# Gets top 15 rows from sorted_rdd
# Writes the top 15 rows to an output file
result = sorted_rdd.map(lambda x: "{0} {1} '{2}' {3} {4}".format(x[0], x[1][0][1][0], x[1][0][0], x[1][0][1][1], x[1][1]))\
    .take(15)
sc.parallelize(result).coalesce(1, shuffle = True).saveAsTextFile(output)

sc.stop()

"""
Sample Output:
B002I0JZOC 46 '12 25, 2012' 4.717391304347826 Sony
B003VANOFY 22 '04 11, 2014' 3.8181818181818183 Logitech
B003EUPCZG 14 '03 7, 2014' 4.357142857142857 Razer
"""