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

from sparktk.tkcontext import TkContext
from sparktk.arguments import require_type


def import_json(file_name, tc=TkContext.implicit):
    """
    Imports a file of JSON records

    JSON records can span multiple lines.  Returns a Frame of one column containing a JSON string per row

    Parameters
    ----------

    :param file_name: file path
    :return: Frame

    Examples
    --------

    Consider a file of JSON records:

        { "obj": {
            "color": "blue",
            "size": 4,
            "shape": "square" }
          }
          { "obj": {
          "color": "green",
          "size": 3,
          "shape": "triangle" }
          }
          { "obj": { "color": "yellow", "size": 5, "shape": "pentagon" } }
          { "obj": {
          "color": "orange",
          "size": 2,
          "shape": "lentil" }
        }

    We can parse this file into a frame of records:

        >>> f = tc.frame.import_json("../datasets/shapes.json")
        >>> f.inspect()
        [#]  records
        =====================================================================
        [0]  { "obj": {
               "color": "blue",
               "size": 4,
               "shape": "square" }
             }
        [1]  { "obj": {
             "color": "green",
             "size": 3,
             "shape": "triangle" }
             }
        [2]  { "obj": { "color": "yellow", "size": 5, "shape": "pentagon" } }
        [3]  { "obj": {
             "color": "orange",
             "size": 2,
             "shape": "lentil" }
             }


    We can further break the JSON records into individual columns with a map_columns (or add_columns) operation:

        >>> import json
        >>> def parse_my_json(row):
        ...     record = json.loads(row.records)['obj']
        ...     return [record['color'], record['size'], record['shape']]

        >>> f2 = f.map_columns(parse_my_json, [('color', str), ('size', int), ('shape', str)])
        >>> f2.inspect()
        [#]  color   size  shape
        ===========================
        [0]  blue       4  square
        [1]  green      3  triangle
        [2]  yellow     5  pentagon
        [3]  orange     2  lentil

    """

    TkContext.validate(tc)
    require_type.non_empty_str(file_name, "file_name")
    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.\
        frame.internal.constructors.ImportMultiLineRecords.importJson(tc.jutils.get_scala_sc(), file_name)
    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)
