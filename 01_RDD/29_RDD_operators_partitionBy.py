# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('hadoop', 1), ('spark', 1), ('hello', 1), ('flink', 1), ('hadoop', 1), ('spark', 1)])

    # 使用partitionBy 自定义 分区
    def divide(k):
        if k == 'hadoop':
            return 0
        elif k == 'spark' or k == 'flink':
            return 1
        else:
            return 2


    print(rdd.partitionBy(3, divide).glom().collect())
    # [[('hadoop', 1), ('hadoop', 1)], [('spark', 1), ('flink', 1), ('spark', 1)], [('hello', 1)]]
