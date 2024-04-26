# coding:utf8
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql import functions as F


if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    sc = spark.sparkContext

    """
    spark.sql.shuffle.partitions 参数指的是, 在sql计算中, shuffle算子阶段默认的分区数.
    对于集群模式来说, 200个默认也算比较合适
    如果在local下运行, 200个很多, 在调度上会带来额外的损耗
    所以在local下建议修改比较低 比如2, 4, 10均可
    这个参数和Spark RDD中设置并行度的参数 是相互独立的.
    """

    # 1. 读取数据集
    schema = StructType().add("user_id", StringType(), nullable=True).\
        add("movie_id", IntegerType(), nullable=True).\
        add("rank", IntegerType(), nullable=True).\
        add("ts", StringType(), nullable=True)
    df = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False).\
        option("encoding", "utf-8").\
        schema(schema=schema).\
        load("hdfs://node1:8020/wjd/sql/u.data")

    df.createOrReplaceTempView("user_movie_rank")

    # 1：用户平均分
    print("用户平均分")
    spark.sql("SELECT user_id, avg(rank) AS avg_rank FROM user_movie_rank GROUP BY user_id ORDER BY avg_rank DESC").show()

    # 2：电影平均分
    print("电影平均分")
    spark.sql("SELECT movie_id, avg(rank) AS avg_rank FROM user_movie_rank GROUP BY movie_id ORDER BY avg_rank DESC").show()

    # 3：查询大于平均分的电影的数量
    print("大于平均分的电影的数量")
    spark.sql("SELECT count(*) FROM (SELECT movie_id, avg(rank) FROM user_movie_rank GROUP BY movie_id HAVING avg(rank) > (SELECT avg(rank) FROM user_movie_rank))").show()

    # 4：查询高分电影中（>3）打分次数最多的用户，并求出此人打的平均分
    print("查询高分电影中（>3）打分次数最多的用户，并求出此人打的平均分")
    spark.sql("SELECT avg(rank) FROM user_movie_rank WHERE user_id = (SELECT user_id FROM user_movie_rank WHERE movie_id in (SELECT movie_id FROM user_movie_rank GROUP BY movie_id HAVING avg(rank) > 3) GROUP BY user_id ORDER BY count(user_id) DESC LIMIT 1)").show()

    # 5：查询每个用户的平均打分，最低打分，最高打分
    print("查询每个用户的平均打分，最低打分，最高打分")
    spark.sql("SELECT user_id, avg(rank) AS avg_rank, min(rank) AS min_rank, max(rank) AS max_rank FROM user_movie_rank GROUP BY user_id ORDER BY avg_rank DESC").show()

    # 6：查询被评分超过100次的电影的平均分排名TOP10
    print("查询被评分超过100次的电影的平均分排名TOP10")
    spark.sql("SELECT movie_id, avg(rank) AS avg_rank FROM user_movie_rank GROUP BY movie_id HAVING count(movie_id) > 100 ORDER BY avg_rank DESC LIMIT 10").show()

    time.sleep(10000)

"""
1. agg: 它是GroupedData对象的API, 作用是 在里面可以写多个聚合
2. alias: 它是Column对象的API, 可以针对一个列 进行改名
3. withColumnRenamed: 它是DataFrame的API, 可以对DF中的列进行改名, 一次改一个列, 改多个列 可以链式调用
4. orderBy: DataFrame的API, 进行排序, 参数1是被排序的列, 参数2是 升序(True) 或 降序 False
5. first: DataFrame的API, 取出DF的第一行数据, 返回值结果是Row对象.
# Row对象 就是一个数组, 你可以通过row['列名'] 来取出当前行中, 某一列的具体数值. 返回值不再是DF 或者GroupedData 或者Column而是具体的值(字符串, 数字等)
"""




