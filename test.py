# encoding=utf8
"""
    Author:  'jdwang'
    Date:    'create date: 2016-08-14'
    Email:   '383287471@qq.com'
    Describe:
"""

import os
import sys

# 添加 环境变量 SPARK的 跟目录
SPARK_HOME = '/home/jdwang/spark-2.0.0-bin-hadoop2.7/'
os.environ['SPARK_HOME'] = SPARK_HOME
# quit()

# Append python spark lib (pyspark)  to Python Path
# 添加两个库
sys.path.extend([
    SPARK_HOME + 'python/lib/pyspark.zip',
    SPARK_HOME + 'python/lib/py4j-0.10.1-src.zip']
)

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

if __name__ == '__main__':
    conf = SparkConf()
    # conf.setMaster("local[3]")
    # 指定具体的Master机器 地址和端口
    conf.setMaster("spark://jdwang-HP:7077")
    conf.setAppName("spark_test")
    # 可以设置属性等
    # conf.set("spark.executor.memory", "12g")
    sc = SparkContext(conf=conf)
    # 测试
    data = sc.textFile('UserPurchaseHistory.csv')
    print(data.map(lambda x: x.split(',')).collect())
