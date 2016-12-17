# encoding=utf8
"""
    Author:  'jdwang'
    Date:    'create date: 2016-12-17'; 'last updated date: 2016-12-17'
    Email:   '383287471@qq.com'
    Describe:
"""
from __future__ import print_function
import sys
import os

# 添加 环境变量 SPARK的 跟目录
SPARK_HOME = '/home/jdwang/spark-2.0.0-bin-hadoop2.7/'
os.environ['SPARK_HOME'] = SPARK_HOME

sys.path.extend([
    SPARK_HOME + 'python/lib/pyspark.zip',
    SPARK_HOME + 'python/lib/py4j-0.10.1-src.zip']
)

from pyspark import SparkContext
from pyspark import SparkConf


if __name__ == '__main__':
    conf = SparkConf()
    conf.setMaster("local[3]")
    # 指定具体的Master机器 地址和端口
    # conf.setMaster("spark://jdwang-HP:7077")
    conf.setAppName("spark_test")
    # 可以设置属性等
    # conf.set("spark.executor.memory", "12g")
    sc = SparkContext(conf=conf)
    # 测试
    from sklearn import svm, datasets
    from spark_sklearn import GridSearchCV

    iris = datasets.load_iris()
    parameters = {'kernel': ('linear', 'rbf'), 'C': [1, 10]}
    svr = svm.SVC()
    clf = GridSearchCV(sc, svr, parameters)
    clf.fit(iris.data, iris.target)
    print(clf.predict(iris.data))
