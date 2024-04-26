# coding:utf8

# SparkSession对象的导包, 对象是来自于 pyspark.sql包中
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    # 通过SparkSession对象可以获取SparkContext对象
    sc = spark.sparkContext

    # SparkSQL的HelloWorld
    df = spark.read.csv("hdfs://node1:8020/wjd/stu_score.txt", sep=',', header=False)
    df2 = df.toDF("id", "name", "score")
    df2.printSchema()  # 输出表结构

    # 打印df中的数据
    # 参数1 表示 展示出多少条数据, 默认不传的话是20
    # 参数2 表示是否对列进行截断, 如果列的数据长度超过20个字符串长度, 后续的内容不显示以...代替
    # 如果给False 表示不阶段全部显示, 默认是True
    df.show(20, False)

    # 将DF对象转换成临时视图表, 可供sql语句查询
    df2.createTempView("score")

    # SQL 风格
    spark.sql("""
        SELECT * FROM score WHERE name='语文' LIMIT 5
    """).show()

    # DSL 风格（pandas风格）
    df2.where("name='语文'").limit(5).show()