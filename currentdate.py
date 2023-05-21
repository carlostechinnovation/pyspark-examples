# -*- coding: utf-8 -*-
"""
¿Como ejecutarlo?
    - Version monoservidor (pandas_normal): python C:\DATOS\GITHUB_REPOS\pyspark-examples\SCRIPT.py
    - Version multiservidor (pandas_api_on_spark): cmd /k C:\apps\spark-3.4.0-bin-hadoop3\bin\spark-submit --master "local[*]" C:\DATOS\GITHUB_REPOS\pyspark-examples\SCRIPT.py
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

schema = StructType([
            StructField("seq", StringType(), True)])

dates = ['1']

df = spark.createDataFrame(list('1'), schema=schema)

df.show()