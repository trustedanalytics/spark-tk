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

import sys
import os

SPARK_HOME="SPARK_HOME"

try:
    import pyspark
except ImportError:
    if SPARK_HOME in os.environ:
        spark_home = os.environ.get(SPARK_HOME)
        pyspark_path = "%s/python" % spark_home
        sys.path.insert(1, pyspark_path)
    else:
        raise Exception("Required Environment variable %s not set" % SPARK_HOME)

from tkcontext import TkContext
from sparkconf import create_sc

import dtypes
from sparktk.loggers import loggers
from sparktk.frame.ops.inspect import inspect_settings


