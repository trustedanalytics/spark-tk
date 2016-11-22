# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

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
            from sparktk.tests import utils
            #from sparktk.loggers import loggers
            #loggers.set("d", "sparktk.sparkconf")
            sc = create_sc(master='local[2]',
                           app_name="pytest-pyspark-local-testing",
                           extra_conf_dict={"spark.hadoop.fs.default.name": "file:///",
                                            "spark.ui.enabled": 'false' })

            request.addfinalizer(lambda: sc.stop())
            global_tc = TkContext(sc)
            global_tc.testing = utils
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

