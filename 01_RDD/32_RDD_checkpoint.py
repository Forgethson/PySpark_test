# coding:utf8
import os
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

os.environ['PYSPARK_PYTHON'] = '/export/server/anaconda3/envs/pyspark/bin/python3.8'
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1. 告知spark, 开启CheckPoint功能
    sc.setCheckpointDir("hdfs://node1:8020/wjd/output/ckp")
    rdd1 = sc.textFile("hdfs://node1:8020/wjd/words.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))

    # 调用checkpoint API 保存数据即可
    rdd3.checkpoint()

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

    rdd3.unpersist()
    time.sleep(100000)
