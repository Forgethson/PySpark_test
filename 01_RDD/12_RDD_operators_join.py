# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([(1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhaoliu")])
    rdd2 = sc.parallelize([(1001, "销售部"), (1002, "科技部")])

    # 通过join算子来进行rdd之间的关联，按照二元元组的key来进行关联
    print(rdd1.join(rdd2).collect())  # inner join，有null行的去掉
    # [(1002, ('lisi', '科技部')), (1001, ('zhangsan', '销售部'))]

    # 左外连接, 右外连接 可以更换一下rdd的顺序 或者调用rightOuterJoin即可
    print(rdd1.leftOuterJoin(rdd2).collect())  # left join，只要left有值就保留
    # [(1002, ('lisi', '科技部')), (1004, ('zhaoliu', None)), (1001, ('zhangsan', '销售部')), (1003, ('wangwu', None))]
