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

def export_to_json(self, path, count=0, offset=0):
    """
    Write current frame to HDFS in Json format.

    Parameters
    ----------

    :param path: (str) The HDFS folder path where the files will be created.
    :param count: (Optional[int]) The number of records you want. Default (0), or a non-positive value, is the
                   whole frame.
    :param offset: (Optional[int]) The number of rows to skip before exporting to the file. Default is zero (0).

    """
    self._scala.exportToJson(path, count, offset)