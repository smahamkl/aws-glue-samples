import datetime

from pyspark.sql.types import IntegerType
from python.udf.DataGenUdf import py_or_udf
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType, IntegerType, ArrayType, DataType, DateType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import rand, randn, round, expr
import random
from python.ColumnFactory import *


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
# if column['column_type'] == 'Sequential':
#    df = df.withColumn(column['name'], (column['start'] + (column['step'] * monotonically_increasing_id())))
# if column['column_type'] == 'Random' and column['data_type'] == 'Date':
#     df = df.withColumn(column['name'],
#                        randdate(lit(column['min']), lit(column['max'])))
# elif column['column_type'] == 'Random' and column['data_type'] != 'Date':
#     try:
#         df = df.withColumn(column['name'],
#                            round(rand() * (column['max'] - column['min']) + column['min'],
#                                  column['decimal_places']))
#     except:
#         df = df.withColumn(column['name'], round(rand() * (column['max'] - column['min']) + column['min'], 0))
# elif column['column_type'] == 'Fixed':
#     df = df.withColumn(column['name'],
#                        lit(column['value']))
# elif column['column_type'] == 'Expression':
#     df = df.withColumn(column['name'],
#                        expr(column['expression']))