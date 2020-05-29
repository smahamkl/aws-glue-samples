from datetime import datetime
import sys
import os
from operator import add
import argparse
import yaml
import functools
from pyspark.sql import SparkSession
from python.DataGen import DataGenerator
import random
from datetime import datetime, timedelta

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.7'
os.environ['SPARK_HOME'] = '/home/sivam/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8'

spark = SparkSession \
    .builder \
    .appName("Data-Faker") \
    .getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--yaml", help="YAML file location",
                    type=str)
parser.add_argument("--out", help="Output data location",
                    type=str)

args = parser.parse_args()

print("YAML location:" + str(args.yaml))
print("Data output location:" + str(args.out))

spark.sparkContext.setLogLevel("OFF")

dataGenerator = DataGenerator(spark)

with open(str(args.yaml)) as file:
    table_list = yaml.load(file, Loader=yaml.FullLoader)
    dataGenerator.generateAndWriteDataFromSchema(table_list, str(args.out))
