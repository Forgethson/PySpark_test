# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('a', 1)])
    # 对元组val+1
    print(rdd.map(lambda x: (x[0], x[1] + 1)).collect())
    print(rdd.mapValues(lambda x: x + 1).collect())
