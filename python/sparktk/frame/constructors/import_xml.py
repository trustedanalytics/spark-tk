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


def import_xml(file_name, record_tag, tc=TkContext.implicit):
    """
    Imports a file of XML records

    XML records can span multiple lines.  Returns a Frame of one column containing a XML string per row

    Note: Only records which start with the given tag will be included (multiple different tags not supported)

    Parameters
    ----------

    :param file_name: file path
    :param record_tag: value of the XML element which contains a record
    :return: Frame

    Examples
    --------

    Consider a file of XML records:

        <?xml version="1.0" encoding="UTF-8"?>
        <table>
            <shape type="triangle">
                <x>0</x>
                <y>0</y>
                <size>12</size>
            </shape>
            <shape type="square">
                <x>8</x>
                <y>0</y>
                <size>4</size>
            </shape>
            <shape color="blue" type="pentagon">
                <x>0</x>
                <y>10</y>
                <size>2</size>
            </shape>
            <shape type="square">
                <x>-4</x>
                <y>6</y>
                <size>7</size>
            </shape>
        </table>

    We can parse this file into a frame of records:

        >>> f = tc.frame.import_xml("../datasets/shapes1.xml", "shape")
        >>> f.inspect()
        [#]  records
        =========================================
        [0]  <shape type="triangle">
                     <x>0</x>
                     <y>0</y>
                     <size>12</size>
                 </shape>
        [1]  <shape type="square">
                     <x>8</x>
                     <y>0</y>
                     <size>4</size>
                 </shape>
        [2]  <shape color="blue" type="pentagon">
                     <x>0</x>
                     <y>10</y>
                     <size>2</size>
                 </shape>
        [3]  <shape type="square">
                     <x>-4</x>
                     <y>6</y>
                     <size>7</size>
                 </shape>


    We can further break the XML records into individual columns with a map_columns (or add_columns) operation:


        >>> import xml.etree.ElementTree as ET
        >>> def parse_my_xml(row):
        ...     ele = ET.fromstring(row[0])
        ...     return [ele.get("type"), int(ele.find("x").text), int(ele.find("y").text), int(ele.find("size").text)]

        >>> f2 = f.map_columns(parse_my_xml, [('shape', str), ('x', int), ('y', int), ('size', int)])

        >>> f2.inspect()
        [#]  shape     x   y   size
        ===========================
        [0]  triangle   0   0    12
        [1]  square     8   0     4
        [2]  pentagon   0  10     2
        [3]  square    -4   6     7


    Consider another file of XML records, this time with different element names for the records:

        <?xml version="1.0" encoding="UTF-8"?>
        <shapes>
            <triangle>
                <x>0</x>
                <y>0</y>
                <size>12</size>
            </triangle>
            <square>
                <x>8</x>
                <y>0</y>
                <size>4</size>
            </square>
            <pentagon color="blue">
                <x>0</x>
                <y>10</y>
                <size>2</size>
            </pentagon>
            <square>
                <x>-4</x>
                <y>6</y>
                <size>7</size>
            </square>
        </shapes>

    We can parse this file into a frame of records of a single type.  We must pick only one.  The others
    will be filtered out:

        >>> f3 = tc.frame.import_xml("../datasets/shapes2.xml", "square")
        >>> f3.inspect()
        [#]  records
        ===========================
        [0]  <square>
                     <x>8</x>
                     <y>0</y>
                     <size>4</size>
                 </square>
        [1]  <square>
                     <x>-4</x>
                     <y>6</y>
                     <size>7</size>
                 </square>


    We can further break the XML records into individual columns with a map_columns (or add_columns) operation:


        >>> def parse_my_squares(row):
        ...     ele = ET.fromstring(row[0])
        ...     return [int(ele.find("x").text), int(ele.find("y").text), int(ele.find("size").text)]

        >>> f4 = f3.map_columns(parse_my_squares, [('x', int), ('y', int), ('size', int)])

        >>> f4.inspect()
        [#]  x   y  size
        ================
        [0]   8  0     4
        [1]  -4  6     7

    """

    TkContext.validate(tc)
    require_type.non_empty_str(file_name, "file_name")
    require_type.non_empty_str(record_tag, "record_tag")
    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.\
        frame.internal.constructors.ImportMultiLineRecords.importXml(tc.jutils.get_scala_sc(), file_name, record_tag)
    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)
