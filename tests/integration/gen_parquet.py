from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext

import json
import sys

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")

input_filename = sys.argv[1]
print(input_filename)
output_filename = sys.argv[2]

with open(input_filename, 'r') as f:
    sqlContext.read.json(
      sc.parallelize(json.load(f))
    ).coalesce(1).write.parquet(output_filename)

    spark.read.parquet(output_filename).printSchema()
