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

# coding: utf-8
import sparktk
from sparktk import TkContext
tc= TkContext()
file_path = "file:///home/kvadla/spark-tk/spark-tk/integration-tests/datasets/cities.csv"
frame = tc.frame.import_csv(file_path, "|", header=True)
frame.count()
destPath = "file:///home/kvadla/spark-tk/spark-tk/integration-tests/tests/sandbox/output26.tfr"
frame.export_to_tensorflow(destPath)
