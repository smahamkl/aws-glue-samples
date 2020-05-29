from abc import ABCMeta, abstractmethod
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType, IntegerType, ArrayType, DataType, DateType, DoubleType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import rand, randn, round, expr
from random import randint
import random
from datetime import datetime, timedelta


class ColumnType(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, spark):
        pass

    @abstractmethod
    def create(self, df, **kwargs):
        pass

    def castColumn(self, df, colName, dataType):
        if dataType == "Int":
            df = df.withColumn(colName, df[colName].cast(IntegerType()))
        elif dataType == "Float":
            df = df.withColumn(colName, df[colName].cast(DoubleType()))
        else:
            df = df.withColumn(colName, df[colName].cast(StringType()))

        return df


class Sequential(ColumnType):
    def __init__(self, spark):
        self.spark = spark

    def create(self, df, **kwargs):
        df = df.withColumn(kwargs.get("name"),
                           (kwargs.get("start") + (kwargs.get("step") * monotonically_increasing_id())))


        return df


class Fixed(ColumnType):
    def __init__(self, spark):
        self.spark = spark

    def create(self, df, **kwargs):
        df = df.withColumn(kwargs.get("name"),
                           lit(kwargs.get("value")))
        return df


class Random(ColumnType):
    def __init__(self, spark):
        self.spark = spark

    def create(self, df, **kwargs):
        def randdates(date1, date2):
            format1 = '%Y-%m-%d %H:%M:%S.%f'
            stime = datetime.strptime(str(date1), format1)
            etime = datetime.strptime(str(date2), format1)

            dt = random.random() * (etime - stime) + stime
            return str(dt)

        def randitemfromlist(listStr):
            try:
                if listStr == "" or listStr is None:
                    return ""
                my_list = listStr.split(",")
                index = randint(0, len(my_list) - 1)
                return str(my_list[index])
            except:
                return ""

        if kwargs.get("dataType") == 'Date':
            randdate = udf(lambda x, y: randdates(x, y), StringType())
            self.spark.udf.register("randdate", randdate)
            df = df.withColumn(kwargs.get("name"), randdate(lit(kwargs.get("min")), lit(kwargs.get("max"))))
            #df = df.withColumn(kwargs.get("name"), df[kwargs.get("name")].cast(TimestampType()))
        elif kwargs.get("randomValuesSr") is not None:
            randvalue = udf(lambda x: randitemfromlist(x), StringType())
            self.spark.udf.register("randvalue", randvalue)
            df = df.withColumn(kwargs.get("name"), randvalue(lit(kwargs.get("randomValuesSr"))))
        else:
            try:
                df = df.withColumn(kwargs.get("name"),
                                   round(rand() * (kwargs.get('max') - kwargs.get('min')) + kwargs.get('min'),
                                         kwargs.get('decimal_places')))
            except:
                df = df.withColumn(kwargs.get("name"),
                                   round(rand() * (kwargs.get('max') - kwargs.get('min')) + kwargs.get('min'),
                                         0))
        return df


class Expression(ColumnType):
    def __init__(self, spark):
        self.spark = spark

    def create(self, df, **kwargs):
        df = df.withColumn(kwargs.get('name'),
                           expr(kwargs.get('expression')))
        return df


class ColumnTypeFactory:
    def __init__(self, spark):
        self.spark = spark

    def createColumn(self, columnType, df, **kwargs):
        obj = eval(columnType)(self.spark)
        return obj.castColumn(obj.create(df, **kwargs), kwargs.get('name'), kwargs.get('dataType'))
