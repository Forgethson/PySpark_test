# coding:utf8
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)


    def func(x):
        print("执行map操作")
        return x, 1


    rdd1 = sc.textFile("hdfs://node1/wjd/words2.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    # rdd3 = rdd2.map(lambda x: (x, 1))
    rdd3 = rdd2.map(func)
    rdd3.cache()
    # rdd3.persist(StorageLevel.MEMORY_AND_DISK_2)
    rdd6 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd6.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

    rdd3.unpersist()
    time.sleep(100000)
