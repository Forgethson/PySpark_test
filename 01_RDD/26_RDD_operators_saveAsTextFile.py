# coding:utf8
import os

from pyspark import SparkConf, SparkContext

os.environ['PYSPARK_PYTHON'] = '/export/server/anaconda3/envs/pyspark/bin/python3.8'
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)
    rdd.saveAsTextFile("hdfs://node1:8020/output/out1")
