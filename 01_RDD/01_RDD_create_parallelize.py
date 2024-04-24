# coding:utf8

# 导入Spark的相关包
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 0. 初始化执行环境 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 演示通过并行化集合的方式去创建RDD, 本地集合 -> 分布式对象(RDD)
    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    # parallelize方法, 没有给定分区数, 默认分区数是多少?  根据CPU核心来定
    print("默认分区数: ", rdd1.getNumPartitions())

    rdd2 = sc.parallelize([1, 2, 3], 3)
    print("分区数: ", rdd2.getNumPartitions())

    # collect方法, 是将RDD(分布式对象)中每个分区的数据, 都发送到Driver中, 形成一个Python List对象
    # collect: 分布式 -> 本地集合
    print("rdd的内容是: ", rdd2.collect())

    print(sc.parallelize(["a", "b", "c"]).collect())  # list
    print(sc.parallelize({"a": 1, "b": 2, "c": 3}).collect())  # dict
    print(sc.parallelize({"a", "b", "c"}).collect())  # set
    print(sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('a', 1)]).collect())  # tuple
    print(sc.parallelize([["a"], ["b"], ["c"]]).collect())  # list
