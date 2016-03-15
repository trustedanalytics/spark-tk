import pytest

from pyspark import SparkConf, SparkContext
from sparktk import Context

jars = [
        '/home/blbarker/dev/tap-at/spark-tk/target/spark-tk-1.0-SNAPSHOT.jar',
        '/home/blbarker/.m2/repository/org/apache/commons/commons-csv/1.0/commons-csv-1.0.jar',
        '/home/blbarker/.m2/repository/io/spray/spray-json_2.10/1.2.6/spray-json_2.10-1.2.6.jar',
        '/home/blbarker/.m2/repository/org/apache/mahout/mahout-math/0.9-cdh5.5.0/mahout-math-0.9-cdh5.5.0.jar',
        '/home/blbarker/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar',
]

jars_comma = ",".join(jars)
jars_colon = ":".join(jars)

import os
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars %s --driver-class-path %s pyspark-shell" % (jars_comma, jars_colon)


# Establish a spark context for the test session
@pytest.fixture(scope="session")
def tk_context(request):
    conf = SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing")
    print "============================================================="
    print conf.toDebugString()
    print "============================================================="

    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())
    return Context(sc)


# def test_smoke_sparktk(spark_context):
#     from sparktk import Context
#     c = Context(spark_context)
#     f = c.to_frame([1,2, 3, 4])
#     assert f.rdd.count() == 4




