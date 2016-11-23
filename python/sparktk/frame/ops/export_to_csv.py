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

def export_to_csv(self, file_name, separator=','):
    """
    Write current frame to disk as a CSV file

    Parameters
    ----------

    :param file_name: (str) file destination
    :param separator: (str) string to be used for delimiting the fields

    Example
    -------

        >>> frame = tc.frame.create([[1, 2, 3], [4, 5, 6]])
        >>> frame.export_to_csv("sandbox/export_example.csv")
        >>> frame2 = tc.frame.import_csv("sandbox/export_example.csv")
        <hide>
        >>> frame2.sort("C0")
        </hide>
        >>> frame2.inspect()
        [#]  C0  C1  C2
        ===============
        [0]   1   2   3
        [1]   4   5   6

    """
    self._scala.exportToCsv(file_name, separator)
