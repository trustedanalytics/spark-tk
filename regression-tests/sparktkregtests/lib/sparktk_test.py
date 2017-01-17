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

"""Setup up tests for regression """

import unittest
import uuid
import datetime
import os
from ConfigParser import SafeConfigParser
from threading import Lock

import sparktk as stk

import config
import spark_context_config

udf_lib_path = os.path.join(config.root, "regression-tests", "sparktkregtests", "lib" ,"udftestlib")
udf_files = [os.path.join(udf_lib_path, f) for f in os.listdir(udf_lib_path)]

lock = Lock()
global_tc = None


def get_context():
    global global_tc
    with lock:
        if global_tc is None:
            sparktkconf_dict = spark_context_config.get_spark_conf()
            if config.run_mode:
                global_tc = stk.TkContext(master='yarn-client', extra_conf_dict=sparktkconf_dict, py_files=udf_files)
            else:
                global_tc = stk.TkContext(py_files=udf_files)

        return global_tc


class SparkTKTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Build the context for use"""
        cls.context = get_context()
        cls.context.sc.setCheckpointDir(config.checkpoint_dir)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def get_file(self, filename, performance_file=False):
        """Return the hdfs path to the given file"""
        # Note this is an HDFS path, not a userspace path. os.path library
        # may be wrong
        if performance_file:
            config_reader = SafeConfigParser() 
            filepath = os.path.abspath(os.path.join(
                config.root, "regression-tests", "sparktkregtests", "lib", "performance.ini"))
            config_reader.read(filepath)
            placed_path = config.performance_data_dir + "/" + config_reader.get(config.test_size, filename)
        else:
            placed_path = config.hdfs_data_dir + "/" + filename
        return placed_path

    def get_export_file(self, filename):
        # Note this is an HDFS path, not a userspace path. os.path library
        # may be wrong
        placed_path = config.export_dir + "/" + filename
        return placed_path

    def get_name(self, prefix):
        """build a guid hardened unique name """
        datestamp = datetime.datetime.now().strftime("%m_%d_%H_%M_")
        name = prefix + datestamp + uuid.uuid1().hex
        return name

    def get_local_dataset(self, dataset):
        """gets the dataset from the dataset folder"""
        dataset_directory = config.dataset_directory
        return os.path.join(dataset_directory, dataset)

    def assertFramesEqual(self, frame1, frame2):
        frame1_take = frame1.take(frame1.count())
        frame2_take = frame2.take(frame2.count())

        self.assertItemsEqual(frame1_take, frame2_take)
