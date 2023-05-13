import findspark
findspark.init()
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg
from pathlib import Path

# conf = SparkConf().setAppName("foo-example")
# sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.format(
        "csv"
    ).option(
        "header", "true"
    ).load(
        "hdfs://node-master:9000/user/root/f1/circuits.csv"
    )

df.printSchema()
