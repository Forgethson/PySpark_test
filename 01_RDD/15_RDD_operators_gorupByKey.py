# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('b', 1)])
    rdd2 = rdd.groupByKey()
    print(rdd2.mapValues(lambda x: list(x)).collect())  # [('a', [1, 1]), ('b', [1, 1, 1])]
