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


def export_to_hbase(self, table_name, key_column_name=None, family_name="familyColumn"):
    """
    Write current frame to HBase table.

    Table must exist in HBase.

    Parameters
    ----------

    :param table_name: (str) The name of the HBase table that will contain the exported frame
    :param key_column_name: (Optional[str]) The name of the column to be used as row key in hbase table
    :param family_name: (Optional[str]) The family name of the HBase table that will contain the exported frame

    Example
    -------

    <skip>
        >>> data = [[1, 0.2, -2, 5], [2, 0.4, -1, 6], [3, 0.6, 0, 7], [4, 0.8, 1, 8]]
        >>> schema = [('a', int), ('b', float),('c', int) ,('d', int)]
        >>> my_frame = tc.frame.create(data, schema)
        >>> my_frame.export_to_hbase("test_demo_hbase", family_name="test_family")
        <progress>
    </skip>

    Verify exported frame in hbase

    From bash shell

        $hbase shell

        hbase(main):001:0> list

    You should see test_demo_hbase table.

    Run hbase(main):001:0> scan 'test_demo_hbase' (to verify frame).

    Output:

        ROW     COLUMN+CELL
         0      column=test_family:a, timestamp=1464219662295, value=1
         0      column=test_family:b, timestamp=1464219662295, value=0.2
         0      column=test_family:c, timestamp=1464219662295, value=-2
         0      column=test_family:d, timestamp=1464219662295, value=5
         1      column=test_family:a, timestamp=1464219662295, value=2
         1      column=test_family:b, timestamp=1464219662295, value=0.4
         1      column=test_family:c, timestamp=1464219662295, value=-1
         1      column=test_family:d, timestamp=1464219662295, value=6
         2      column=test_family:a, timestamp=1464219662295, value=3
         2      column=test_family:b, timestamp=1464219662295, value=0.6
         2      column=test_family:c, timestamp=1464219662295, value=0
         2      column=test_family:d, timestamp=1464219662295, value=7
         3      column=test_family:a, timestamp=1464219662295, value=4
         3      column=test_family:b, timestamp=1464219662295, value=0.8
         3      column=test_family:c, timestamp=1464219662295, value=1
         3      column=test_family:d, timestamp=1464219662295, value=8
        4 row(s) in 0.1560 seconds
    """
    if not isinstance(table_name, basestring):
        raise ValueError("Unsupported 'table_name' parameter type.  Expected string, but found %s." % type(table_name))

    if not isinstance(family_name, basestring):
        raise ValueError(
            "Unsupported 'family_name' parameter type.  Expected string, but found %s." % type(family_name))

    self._scala.exportToHbase(table_name, self._tc.jutils.convert.to_scala_option(key_column_name), family_name)
