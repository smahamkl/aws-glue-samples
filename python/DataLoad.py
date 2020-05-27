import sys
import os
import yaml
import functools
from pyspark.sql import SparkSession
import datetime

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType, IntegerType, ArrayType, DataType, DateType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import rand, randn, round, expr
import random
from abc import ABCMeta, abstractmethod
import boto3
from awsglue.utils import getResolvedOptions


class DataGenerator:
    def __init__(self, spark):
        self.spark = spark

    def checkKey(self, key):
        try:
            if key:
                return key
        except:
            return None

    def generateAndWriteDataFromSchema(self, schema, out):
        print(schema)

        def randdates(date1, date2):
            try:
                start_date = datetime.datetime.strptime(str(date1), '%Y-%m-%d')
                end_date = datetime.datetime.strptime(str(date2), '%Y-%m-%d')

                time_between_dates = end_date.date() - start_date.date()
                days_between_dates = time_between_dates.days

                random_number_of_days = random.randrange(days_between_dates)
                return str(start_date.date() + datetime.timedelta(days=random_number_of_days))
            except:
                return ""

        for table in schema['tables']:
            print("--------------" + table['name'] + "--------------------")

            df = self.spark.range(table['rows']).select(col("id").cast("double"))
            df = df.repartition(1)

            randdate = udf(lambda x, y: randdates(x, y), StringType())
            self.spark.udf.register("randdate", randdate)
            colFactory = ColumnTypeFactory(self.spark)

            for column in table['columns']:
                start = 0;
                min = 0;
                max = 0;
                step = 0;
                decimalPlaces = 0;
                expression = None;
                dataType = None
                value = None
                try:
                    start = column['start']
                    step = column['step']
                except:
                    pass
                try:
                    min = column['min']
                    max = column['max']
                except:
                    pass
                try:
                    dataType = column['data_type']
                except:
                    pass
                try:
                    decimalPlaces = column['decimal_places']
                except:
                    pass
                try:
                    expression = column['expression']
                except:
                    pass
                try:
                    value = column['value']
                except:
                    pass

                df = colFactory.createColumn(column['column_type'], df, start=start, name=column['name'],
                                             dataType=dataType,
                                             min=min, max=max, expression=expression,
                                             decimalPlaces=decimalPlaces,
                                             step=step, value=value)

            df = df.drop('id')
            df.show()
            df.write.format('parquet').mode('OverWrite').save(out + '/' + table['name'])


class ColumnType(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, spark):
        pass

    @abstractmethod
    def create(self, df, **kwargs):
        pass


class Sequential(ColumnType):
    def __init__(self, spark):
        self.spark = spark

    def create(self, df, **kwargs):
        df = df.withColumn(kwargs.get("name"),
                           (kwargs.get("start") + (kwargs.get("step") * monotonically_increasing_id())))
        if kwargs.get("dataType") == "Int":
            df = df.withColumn(kwargs.get("name"), df[kwargs.get("name")].cast(IntegerType()))

        return df


class Fixed(ColumnType):
    def __init__(self, spark):
        self.spark = spark

    def create(self, df, **kwargs):
        df = df.withColumn(kwargs.get("name"),
                           lit(kwargs.get("value")))
        if kwargs.get("dataType") == "Int":
            df = df.withColumn(kwargs.get("name"), df[kwargs.get("name")].cast(IntegerType()))
        return df


class Random(ColumnType):
    def __init__(self, spark):
        self.spark = spark

    def create(self, df, **kwargs):
        def randdates(date1, date2):
            try:
                start_date = datetime.datetime.strptime(str(date1), '%Y-%m-%d')
                end_date = datetime.datetime.strptime(str(date2), '%Y-%m-%d')

                time_between_dates = end_date.date() - start_date.date()
                days_between_dates = time_between_dates.days

                random_number_of_days = random.randrange(days_between_dates)
                return str(start_date.date() + datetime.timedelta(days=random_number_of_days))
            except:
                return ""

        if kwargs.get("dataType") == 'Date':
            randdate = udf(lambda x, y: randdates(x, y), StringType())
            self.spark.udf.register("randdate", randdate)
            df = df.withColumn(kwargs.get("name"), randdate(lit(kwargs.get("min")), lit(kwargs.get("max"))))
        elif kwargs.get("dataType") != 'Date':
            try:
                df = df.withColumn(kwargs.get("name"),
                                   round(rand() * (kwargs.get('max') - kwargs.get('min')) + kwargs.get('min'),
                                         kwargs.get('decimal_places')))
            except:
                df = df.withColumn(kwargs.get("name"),
                                   round(rand() * (kwargs.get('max') - kwargs.get('min')) + kwargs.get('min'),
                                         0))
        if kwargs.get("dataType") == "Int":
            df = df.withColumn(kwargs.get("name"), df[kwargs.get("name")].cast(IntegerType()))
        return df


class Expression(ColumnType):
    def __init__(self, spark):
        self.spark = spark

    def create(self, df, **kwargs):
        df = df.withColumn(kwargs.get('name'),
                           expr(kwargs.get('expression')))
        if kwargs.get("dataType") == "Int":
            df = df.withColumn(kwargs.get("name"), df[kwargs.get("name")].cast(IntegerType()))
        return df


class ColumnTypeFactory:
    def __init__(self, spark):
        self.spark = spark

    def createColumn(self, columnType, df, **kwargs):
        obj = eval(columnType)(self.spark)
        return obj.create(df, **kwargs)


args = getResolvedOptions(sys.argv,
                          ['OUT_PATH'])

spark = SparkSession \
    .builder \
    .appName("Data-Faker") \
    .getOrCreate()

print("The output path is: " + args['OUT_PATH'])

spark.sparkContext.setLogLevel("OFF")

dataGenerator = DataGenerator(spark)

bucket = "glue-scripts-6688271755"
s3_client = boto3.client('s3')
response = s3_client.get_object(Bucket=bucket, Key="example.yaml")

table_list = yaml.safe_load(response["Body"])
dataGenerator.generateAndWriteDataFromSchema(table_list, args['OUT_PATH'])

# with open(str(args.yaml)) as file:
#    table_list = yaml.load(file, Loader=yaml.FullLoader)
#    dataGenerator.generateAndWriteDataFromSchema(table_list, "abc")
