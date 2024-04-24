# coding:utf8
import os

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("WordCountHelloWorld").setMaster("local[*]")
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    # 需求 : wordcount单词计数, 读取HDFS上的words.txt文件, 对其内部的单词统计出现 的数量
    # 读取文件
    file_rdd = sc.textFile("hdfs://node1:8020/wjd/words.txt")
    # file_rdd = sc.textFile("../data/input/words.txt") 不要使用本地文件，有可能别的机器没有
    print(file_rdd.collect())

    # 将单词按照空格进行切割, 得到一个存储全部单词的集合对象
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))
    print(words_rdd.collect())

    # 将单词转换为元组对象, key是单词, value是数字1
    words_with_one_rdd = words_rdd.map(lambda x: (x, 1))
    print(words_with_one_rdd.collect())

    # 将元组的value 按照key来分组, 对所有的value执行聚合操作(相加)
    result_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # 简写
    # result_rdd = (sc.textFile("hdfs://node1:8020/wjd/words.txt")
    #               .flatMap(lambda line: line.split(" "))
    #               .map(lambda x: (x, 1))
    #               .reduceByKey(lambda a, b: a + b))

    # 通过collect方法收集RDD的数据打印输出结果
    print(result_rdd.collect())
