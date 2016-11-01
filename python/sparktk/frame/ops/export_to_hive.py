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

def export_to_hive(self, hive_table_name):
    """
    Write current frame to Hive table.

    Table must not exist in Hive. Hive does not support case sensitive table names and columns names.
    Hence column names with uppercase letters will be converted to lower case by Hive.

    Parameters
    ----------

    :param hive_table_name: (str) hive table name

    Example
    --------
        <skip>
        >>> data = [[1, 0.2, -2, 5], [2, 0.4, -1, 6], [3, 0.6, 0, 7], [4, 0.8, 1, 8]]
        >>> schema = [('a', int), ('b', float),('c', int) ,('d', int)]
        >>> my_frame = tc.frame.create(data, schema)
        <progress>

        </skip>

    table_name: (string): table name. It will create new table with given name if it does not exists already.

    <skip>
        >>> my_frame.export_to_hive("demo_test_hive")
        <progress>

    </skip>

    Verify exported frame in hive

    From bash shell

        $hive
        hive> show tables

    You should see demo_test_hive table.

    Run hive> select * from demo_test_hive; (to verify frame).

    """
    self._scala.exportToHive(hive_table_name)
