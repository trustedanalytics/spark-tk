import pytest

from threading import Lock
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
]

jars_comma = ",".join(jars)
jars_colon = ":".join(jars)

pyspark_submit_args = "--driver-class-path %s pyspark-shell" % jars_colon
print "[setup.py] Setting $PYSPARK_SUBMIT_ARGS=%s" % pyspark_submit_args
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

lock = Lock()
global_tk_context = None

# Establish a sparktk Context with SparkContext for the test session
@pytest.fixture(scope="session")
def tk_context(request):
    global global_tk_context
    with lock:
        if global_tk_context is None:
            conf = SparkConf().setMaster(master).setAppName("pytest-pyspark-local-testing")
            print "=" * 80
            print "SparkConf"
            print conf.toDebugString()
            print "=" * 80

            sc = SparkContext(conf=conf)
            request.addfinalizer(lambda: sc.stop())
            global_tk_context =  Context(sc)
    return global_tk_context


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

