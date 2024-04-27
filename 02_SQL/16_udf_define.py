# coding:utf8
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql import functions as F

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        getOrCreate()
    sc = spark.sparkContext

    # 构建一个RDD
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7]).map(lambda x: [x])
    df = rdd.toDF(["num"])

    def num_ride_10(num):
        return num * 10

    # 方式1：spark.udf.register()
    udf2 = spark.udf.register("udf1", num_ride_10, IntegerType())

    df.selectExpr("udf1(num)").show()
    df.select(udf2(df['num'])).show()

    # 方式2：F.udf，仅能用于DSL风格
    udf3 = F.udf(num_ride_10, IntegerType())
    df.select(udf3(df['num'])).show()
