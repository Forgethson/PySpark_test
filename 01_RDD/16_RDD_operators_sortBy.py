# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('c', 3), ('f', 1), ('b', 11), ('c', 3), ('a', 1), ('c', 5), ('e', 1), ('n', 9), ('a', 1)], 3)
    # 按照value 数字进行排序
    # 参数1：函数，表示按照数据的哪个列进行排序
    # 参数2: True表示升序 False表示降序
    # 参数3: 排序后RDD的分区数，默认与原RDD的分区数一致
    print(rdd.sortBy(lambda x: x[0], ascending=True).collect())
    # [('a', 1), ('a', 1), ('b', 11), ('c', 3), ('c', 3), ('c', 5), ('e', 1), ('f', 1), ('n', 9)]

