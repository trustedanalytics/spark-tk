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

""" Global Config file for testcases, used heavily by automation"""
import os


root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
dataset_directory = os.path.join(root, "regression-tests", "datasets")
hdfs_namenode = os.getenv("CDH_MASTER", "localhost")
user = os.getenv("USER", "hadoop")
run_mode = True if os.getenv("RUN_MODE", "yarn_client") == "yarn_client" else False
hostname = os.getenv("HOSTNAME")

# HDFS paths, need to be set NOT using os.join since HDFS doesn't use the system
# path seperator, it uses HDFS path seperator ('/')
hdfs_user_root = "/user/" + user
hdfs_data_dir = hdfs_user_root + "/qa_data"
checkpoint_dir = hdfs_user_root + "/sparktk_checkpoint"
export_dir = "hdfs://"+hostname+":8020"+hdfs_user_root+"/sparktk_export"

# Performance test information
test_size = os.getenv("PERFORMANCE_SIZE", "test")
performance_data_dir = hdfs_data_dir if test_size == "test" else "/performance"

scoring_engine_host = os.getenv("SCORING_ENGINE_HOST", "127.0.0.1")
