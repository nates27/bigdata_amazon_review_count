import re
import sys
from pyspark import SparkConf,SparkContext, SQLContext 

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlcontext = SQLContext(sc)

#Script Arguments
review_file = sys.argv[1]
meta_file = sys.argv[2]
output = sys.argv[3]

#Step 1
reviews_rdd =  sqlcontext.read.json(review_file).rdd
asin_time_rdd = reviews_rdd.map(lambda x: ((x['asin'], x['reviewTime']), x['overall']))
counter_rdd = asin_time_rdd.mapValues(lambda x: (x, 1))
reviews_agg_rdd = counter_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + 1))\
    .mapValues(lambda a: (a[1], a[0]/a[1]))

#Step 2
meta_rdd = sqlcontext.read.json(meta_file).rdd
brand_rdd = meta_rdd.map(lambda x: (x['asin'], x['brand']))

#Step 3
review_join = reviews_agg_rdd.map(lambda x: (x[0][0],(x[0][1],x[1])))
rdd_tojoin = review_join.join(brand_rdd).distinct()

#Step 4
sorted_rdd = rdd_tojoin.sortBy(lambda x: -x[1][0][1][0])\
    .map(lambda x: (x[0], x[1][0][1][0], x[1][0][0], x[1][0][1][1], x[1][1]))

with open(output, 'w+') as f:
    for x in sorted_rdd.take(15):
        y = re.sub(' +', ' ', x[4])
        z = re.sub('\n', '', y)
        f.write(f"{x[0]} {x[1]} '{x[2]}' {x[3]} {z}\n")

sc.stop()