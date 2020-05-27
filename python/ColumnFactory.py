from abc import ABCMeta, abstractmethod
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType, IntegerType, ArrayType, DataType, DateType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import rand, randn, round, expr
import random
import datetime


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
# if __name__ == "__main__":
#     ColumnTypeFactory.createColumn(columnType="Expression", name="exprs-col", expression="a/b")
#     ColumnTypeFactory.createColumn(columnType="Random", name="exprs-col", dataType="Int", min=0, max=100000,
#                                    decimalPlaces=2)
