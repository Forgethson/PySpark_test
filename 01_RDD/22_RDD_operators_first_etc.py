# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    print(rdd.first())  # 1
    print(rdd.take(3))  # [1, 2, 3]

    rdd2 = sc.parallelize([8, 6, 3, 2, 4, 9, 7, 1, 5], 3)
    print(rdd2.top(4))  # [9, 8, 7, 6]
    print(rdd2.count())  # 9
