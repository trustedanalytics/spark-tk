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


