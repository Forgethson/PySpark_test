# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 0. 初始化执行环境 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile("hdfs://node1:8020/wjd/apache.log")
    rdd1.cache()

    # 需求1- 计算当前网站访问的 PV(被访问次数)
    print(f'PV = {rdd1.count()}')

    # 需求2- 当前访问的 UV (访问的用户数)
    rdd2 = rdd1.map(lambda x: x.split(" ")[0])
    rdd3 = rdd2.distinct()
    rdd3.cache()
    print(f'UV = {rdd3.count()}')

    # 需求3- 有哪些IP访问了本网站
    print(f'IP = {rdd3.collect()}')

    # 需求4- 哪个页面的访问量最高
    rdd4 = rdd1.map(lambda x: (x.split(" ")[-1], 1))
    rdd5 = rdd4.reduceByKey(lambda x, y: x + y)
    rdd6 = rdd5.sortBy(lambda x: x[1], ascending=False)
    print(f'访问量最高的页面是：{rdd6.take(1)[0][0]}')
