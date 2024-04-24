# coding:utf8
import os

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取小文件文件夹
    rdd = sc.wholeTextFiles("hdfs://node1:8020/wjd/tiny_files")
    print(rdd.collect())
    print(rdd.map(lambda x: x[1]).collect())
