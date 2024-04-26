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

    """读取数据"""
    df = spark.read.format("csv"). \
        option("sep", ";"). \
        option("header", True). \
        load("hdfs://node1:8020/wjd/sql/people.csv")

    # 1. 数据去重：dropDuplicates
    df.dropDuplicates().show()  # 默认按照所有列进行去重，只保留一条
    df.dropDuplicates(['age', 'job']).show()  # 指定age和job列进行去重

    # 2. 缺失值删除：dropna
    df.dropna().show()  # 删除所有包含缺失值的行
    df.dropna(thresh=3).show()  # 至少有3个非空值才保留
    df.dropna(thresh=2, subset=['name', 'age']).show()  # 指定name和age列，至少有2个非空值才保留

    # 3. 缺失值填充
    df.fillna("loss").show()  # 所有缺失值填充为"loss"
    df.fillna("N/A", subset=['job']).show()  # 指定job列填充为"N/A"
    df.fillna({"name": "未知姓名", "age": 1, "job": "worker"}).show()  # 设定一个字典, 对所有的列 提供填充规则
