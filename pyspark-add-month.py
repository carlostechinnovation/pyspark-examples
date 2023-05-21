# -*- coding: utf-8 -*-
"""
¿Como ejecutarlo?
    - Version monoservidor (pandas_normal): python C:\DATOS\GITHUB_REPOS\pyspark-examples\SCRIPT.py
    - Version multiservidor (pandas_api_on_spark): cmd /k C:\apps\spark-3.4.0-bin-hadoop3\bin\spark-submit --master "local[*]" C:\DATOS\GITHUB_REPOS\pyspark-examples\SCRIPT.py
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

from pyspark.sql.functions import col,expr
data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]
spark.createDataFrame(data).toDF("date","increment") \
    .select(col("date"),col("increment"), \
      expr("add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int))").alias("inc_date")) \
    .show()