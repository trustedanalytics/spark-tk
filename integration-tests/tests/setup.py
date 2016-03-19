import pytest

from pyspark import SparkConf, SparkContext
from sparktk import Context
import os
import shutil

d = os.path.dirname
root_path = d(d(d(__file__)))
print "[setup.py] root_path = %s" % root_path

master = "local[2]"
print "[setup.py] master=%s" % master

jars = [
        os.path.join(root_path, 'spark-tk/target/*'),
        os.path.join(root_path, 'spark-tk/target/dependencies/*'),
        #'/home/blbarker/dev/tap-at/spark-tk/target/spark-tk-1.0-SNAPSHOT.jar',
        #'/home/blbarker/.m2/repository/org/apache/commons/commons-csv/1.0/commons-csv-1.0.jar',
        #'/home/blbarker/.m2/repository/io/spray/spray-json_2.10/1.2.6/spray-json_2.10-1.2.6.jar',
        #'/home/blbarker/.m2/repository/org/apache/mahout/mahout-math/0.9-cdh5.5.0/mahout-math-0.9-cdh5.5.0.jar',
        #'/home/blbarker/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar',
]

jars_comma = ",".join(jars)
jars_colon = ":".join(jars)

#pyspark_submit_args = "--jars %s --driver-class-path %s pyspark-shell" % (jars_comma, jars_colon)
pyspark_submit_args = "--driver-class-path %s pyspark-shell" % jars_colon
print "[setup.py] Setting $PYSPARK_SUBMIT_ARGS=%s" % pyspark_submit_args
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args


# Establish a sparktk Context with SparkContext for the test session
@pytest.fixture(scope="session")
def tk_context(request):
    conf = SparkConf().setMaster(master).setAppName("pytest-pyspark-local-testing")
    print "=" * 80
    print "SparkConf"
    print conf.toDebugString()
    print "=" * 80

    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())
    return Context(sc)


sandbox_path = "sandbox"


def get_sandbox_path(path):
    return os.path.join(sandbox_path, path)


def rm(path, error=False):
    try:
        if os.path.isfile(path):
            os.unlink(path)
        elif os.path.isdir(path): shutil.rmtree(path)
    except Exception as e:
        if error:
            raise
        print "[WARN] %s" % e


def clear_folder(folder, warn=False):
    for file_name in os.listdir(folder):
        file_path = os.path.join(folder, file_name)
        rm(file_path, warn)

