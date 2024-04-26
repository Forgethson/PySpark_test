# coding:utf8
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql import functions as F

os.environ['PYSPARK_PYTHON'] = '/export/server/anaconda3/envs/pyspark/bin/python3.8'

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        getOrCreate()
    sc = spark.sparkContext

    # 1. 读取数据集
    schema = StructType().add("user_id", StringType(), nullable=True). \
        add("movie_id", IntegerType(), nullable=True). \
        add("rank", IntegerType(), nullable=True). \
        add("ts", StringType(), nullable=True)
    df = spark.read.format("csv"). \
        option("sep", "\t"). \
        option("header", False). \
        option("encoding", "utf-8"). \
        schema(schema=schema). \
        load("hdfs://node1:8020/wjd/sql/u.data")

    # Write text 写出, 只能写出一个列的数据, 需要将df转换为单列df
    df.select(F.concat_ws("---", "user_id", "movie_id", "rank", "ts")). \
        write. \
        mode("overwrite"). \
        format("text"). \
        save("hdfs://node1:8020/wjd/output/sql/text")

    # Write csv
    df.write.mode("overwrite"). \
        format("csv"). \
        option("sep", ";"). \
        option("header", True). \
        save("hdfs://node1:8020/wjd/output/sql/csv")

    # Write json
    df.write.mode("overwrite"). \
        format("json"). \
        save("hdfs://node1:8020/wjd/output/sql/json")

    # Write parquet
    df.write.mode("overwrite"). \
        format("parquet"). \
        save("hdfs://node1:8020/wjd/output/sql/parquet")
