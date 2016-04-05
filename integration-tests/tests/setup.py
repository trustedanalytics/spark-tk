import pytest
import sys
import os
import shutil
from threading import Lock

d = os.path.dirname
root_path = d(d(d(__file__)))
print "[setup.py] root_path = %s" % root_path

python_folder = os.path.join(root_path, "python")
sys.path.insert(0, python_folder)

lock = Lock()
global_tc = None

# Establish a sparktk Context with SparkContext for the test session
@pytest.fixture(scope="session")
def tc(request):
    global global_tc
    with lock:
        if global_tc is None:
            from sparktk import TkContext
            from sparktk import create_sc
            #from sparktk.loggers import loggers
            #loggers.set("d", "sparktk.sparkconf")
            sc = create_sc(master='local[2]', app_name="pytest-pyspark-local-testing")
            request.addfinalizer(lambda: sc.stop())
            global_tc = TkContext(sc)
    return global_tc


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

