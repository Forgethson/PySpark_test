# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)

    # repartition 修改分区
    print(rdd.repartition(1).getNumPartitions())  # 1
    print(rdd.repartition(5).getNumPartitions())  # 5

    # coalesce 修改分区
    print(rdd.coalesce(1).getNumPartitions())  # 1
    print(rdd.coalesce(5, shuffle=True).getNumPartitions())  # 5
    print(rdd.coalesce(5, shuffle=False).getNumPartitions())  # 3（不进行shuffle，默认，不增加分区）
