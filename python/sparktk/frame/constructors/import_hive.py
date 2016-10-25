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


def import_hive(hive_query, tc=TkContext.implicit):
    """
    Import data from hive table into frame.

    Define the sql query to retrieve the data from a hive table.

    Only a subset of Hive data types are supported:


        DataType    Support
        ----------  ------------------------------------
        boolean     cast to int

        bigint      native support
        int         native support
        tinyint     cast to int
        smallint    cast to int

        decimal     cast to double, may lose precision
        double      native support
        float       native support

        date        cast to string
        string      native support
        timestamp   cast to string
        varchar     cast to string

        arrays      not supported
        binary      not supported
        char        not supported
        maps        not supported
        structs     not supported
        union       not supported


    Parameters
    ----------

    :param hive_query: (str) hive query to fetch data from table
    :param tc: (TkContext) TK context
    :return: (Frame) returns frame with hive table data

    Examples
    --------
    Load data into frame from a hive table based on hive query

    <skip>
        >>> h_query = "select * from demo_test"
        >>> frame = tc.frame.import_hive(h_query)
        -etc-

        >>> frame.inspect()
        [#]  number  strformat
        ======================
        [0]       1  one
        [1]       2  two
        [2]       3  three
        [3]       4  four
    </skip>

    """
    if not isinstance(hive_query, basestring):
        raise ValueError("hive query parameter must be a string, but is {0}.".format(type(hive_query)))
    TkContext.validate(tc)

    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.constructors.Import.importHive(tc.jutils.get_scala_sc(), hive_query)

    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)
