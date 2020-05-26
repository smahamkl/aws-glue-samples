from __future__ import print_function
import sys
import os
from random import random
from operator import add
# Connect to Spark by creating a Spark session
from pyspark.sql import SparkSession

#os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/Anaconda/bin/python'
#os.environ['PYSPARK_PYTHON'] = '/home/sivam/anaconda3/bin/python'
#os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.7'

spark = SparkSession\
    .builder\
    .appName("PythonPi")\
    .getOrCreate()

partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
n = 100000 * partitions

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 < 1 else 0

# To access the associated SparkContext
count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
print("Pi is roughly %f" % (4.0 * count / n))

spark.stop()