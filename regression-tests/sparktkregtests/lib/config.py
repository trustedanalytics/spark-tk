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


qa_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
hdfs_namenode = os.getenv("CDH_MASTER", "localhost")
user = os.getenv("USER", "hadoop")
run_mode = True if os.getenv("RUN_MODE","1") == "1" else False # (0=local, 1=Yarnclient)
#run_mode = False if os.getenv("RUN_MODE","0") == "0" else True
