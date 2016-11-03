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
from pyspark.rdd import RDD
import sparktk.dtypes as dtypes
from sparktk.arguments import require_type


def import_csv_raw(path, delimiter=",", header=False, tc=TkContext.implicit):
    """
    Creates a frame by importing the data as strings from the specified csv file.  If the csv file has a header row,
    those values will be used as column names.  Otherwise, columns will be named generically, like 'C0', 'C1', 'C2', etc.

    Parameters
    ----------

    :param path: (str) Full path to the csv file
    :param delimiter: (str) A string which indicates the separation of data fields.  This is usually a single character
                      and could be a non-visible character, such as a tab. The default delimiter is a comma (,).
    :param header: (bool) Boolean value indicating if the first line of the file will be used to name columns, and not
                   be included in the data.  The default value is false.
    :return: (Frame) Frame that contains the data from the csv file

    Examples
    --------

    Import raw data from a csv file by specifying the path to the file, delimiter, and header option.  All data will
    be brought in the the frame as strings, and columns will be named according to the header row, if there was one.

        >>> file_path = "../datasets/cities.csv"

        >>> frame = tc.frame.import_csv_raw(file_path, delimiter="|", header=True)
        -etc-

        >>> frame.inspect()
        [#]  rank  city         population_2013  population_2010  change  county
        ============================================================================
        [0]  1     Portland     609456           583776           4.40%   Multnomah
        [1]  2     Salem        160614           154637           3.87%   Marion
        [2]  3     Eugene       159190           156185           1.92%   Lane
        [3]  4     Gresham      109397           105594           3.60%   Multnomah
        [4]  5     Hillsboro    97368            91611            6.28%   Washington
        [5]  6     Beaverton    93542            89803            4.16%   Washington
        [6]  15    Grants Pass  35076            34533            1.57%   Josephine
        [7]  16    Oregon City  34622            31859            8.67%   Clackamas
        [8]  17    McMinnville  33131            32187            2.93%   Yamhill
        [9]  18    Redmond      27427            26215            4.62%   Deschutes

        >>> frame.schema
        [('rank', <type 'str'>), ('city', <type 'str'>), ('population_2013', <type 'str'>), ('population_2010', <type 'str'>), ('change', <type 'str'>), ('county', <type 'str'>)]


    """
    TkContext.validate(tc)
    require_type.non_empty_str(path, "path")
    require_type.non_empty_str(delimiter, "delimiter")
    require_type(bool, header, "header")

    df = tc.sql_context.read.format(
        "com.databricks.spark.csv.org.trustedanalytics.sparktk").options(
        delimiter=delimiter,
        header=str(header).lower(),
        inferschema="false").load(path, schema=None)

    df_schema = []

    for column in df.schema.fields:
        try:
            datatype = dtypes.dtypes.get_primitive_type_from_pyspark_type(type(column.dataType))
        except ValueError:
            raise TypeError("Unsupported data type ({0}) for column {1}.".format(str(column.dataType), column.name))
        df_schema.append((column.name, datatype))

    jrdd = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.rdd.PythonJavaRdd.scalaToPython(df._jdf.rdd())
    rdd = RDD(jrdd, tc.sc)

    from sparktk.frame.frame import Frame  # circular dependency, so import late
    return Frame(tc, rdd, df_schema)