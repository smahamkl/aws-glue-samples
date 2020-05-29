import datetime

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType, IntegerType, ArrayType, DataType, DateType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import rand, randn, round, expr
import random
from python.ColumnFactory import *


class DataGenerator:
    def __init__(self, spark):
        self.spark = spark

    def generateAndWriteDataFromSchema(self, schema, out):
        print(schema)

        for table in schema['tables']:
            print("--------------" + table['name'] + "--------------------")

            df = self.spark.range(table['rows']).select(col("id").cast("double"))
            df = df.repartition(1)
            colFactory = ColumnTypeFactory(self.spark)

            for column in table['columns']:
                start = 0
                min = 0
                max = 0
                step = 0
                decimalPlaces = 0
                expression = None
                dataType = None
                value = None
                randomValuesSr = None
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
                try:
                    randomValuesSr = column['values']
                except:
                    pass

                df = colFactory.createColumn(column['column_type'], df, start=start, name=column['name'],
                                             dataType=dataType,
                                             min=min, max=max, expression=expression,
                                             decimalPlaces=decimalPlaces,
                                             step=step, value=value, randomValuesSr=randomValuesSr)

            df = df.drop('id')
            df.show()
            df.write.format('parquet').mode('OverWrite').save(out + '/' + table['name'])