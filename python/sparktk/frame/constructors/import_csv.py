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
from pyspark.sql.types import *
import sparktk.dtypes as dtypes
from datetime import datetime
from sparktk.frame import schema as sparktk_schema
from sparktk.arguments import require_type


def import_csv(path, delimiter=",", header=False, schema=None, datetime_format="yyyy-MM-dd'T'HH:mm:ss.SSSX", tc=TkContext.implicit):
    """
    Creates a frame with data from a csv file.

    Parameters
    ----------

    :param path: (str) Full path to the csv file
    :param delimiter: (Optional[str]) A string which indicates the separation of data fields.  This is usually a
                      single character and could be a non-visible character, such as a tab. The default delimiter
                      is a comma (,).
    :param header: (Optional[bool]) Boolean value indicating if the first line of the file will be used to name columns
                   (unless a schema is provided), and not be included in the data.  The default value is false.
    :param schema: (Optional(list[tuple(str, type)] or list[str])) The are different options for specifying a schema:

                    *  Provide the full schema for the frame as a list of tuples (string column name and data type)
                    *  Provide the column names as a list of strings.  Column data types will be inferred, based on the
                    data.  The column names specified will override column names that are found in the header row.
                    *  None, where the schema is automatically inferred based on the data.  Columns are named based on
                    the header, or will be named generically ("C0", "C1", "C2", etc).

    :param datetime_format: (str) String specifying how date/time columns are formatted, using the java.text.SimpleDateFormat
                        specified at https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
    :return: (Frame) Frame that contains the data from the csv file

    Examples
    --------

    Load a frame from a csv file by specifying the path to the file, delimiter

        >>> file_path = "../datasets/cities.csv"

        >>> frame = tc.frame.import_csv(file_path, "|", header=True)
        -etc-

        >>> frame.inspect()
        [#]  rank  city         population_2013  population_2010  change  county
        ============================================================================
        [0]     1  Portland              609456           583776  4.40%   Multnomah
        [1]     2  Salem                 160614           154637  3.87%   Marion
        [2]     3  Eugene                159190           156185  1.92%   Lane
        [3]     4  Gresham               109397           105594  3.60%   Multnomah
        [4]     5  Hillsboro              97368            91611  6.28%   Washington
        [5]     6  Beaverton              93542            89803  4.16%   Washington
        [6]    15  Grants Pass            35076            34533  1.57%   Josephine
        [7]    16  Oregon City            34622            31859  8.67%   Clackamas
        [8]    17  McMinnville            33131            32187  2.93%   Yamhill
        [9]    18  Redmond                27427            26215  4.62%   Deschutes

        >>> frame.schema
        [('rank', <type 'int'>), ('city', <type 'str'>), ('population_2013', <type 'int'>), ('population_2010', <type 'int'>), ('change', <type 'str'>), ('county', <type 'str'>)]

    The schema parameter can be used to specify a custom schema (column names and data types) or column names (and the
    data types are inferred based on the data).  Here, we will specify the column names, which will override the
    header from the csv file.

        >>> column_names = ["Rank", "City", "2013", "2010", "Percent_Change", "County"]
        >>> frame = tc.frame.import_csv(file_path, "|", header=True, schema=column_names)
        -etc-

        >>> frame.schema
        [('Rank', <type 'int'>), ('City', <type 'str'>), ('2013', <type 'int'>), ('2010', <type 'int'>), ('Percent_Change', <type 'str'>), ('County', <type 'str'>)]

        <hide>
        >>> file_path = "../datasets/unicode.csv"
        >>> schema = [("a", unicode),("b", unicode),("c",unicode)]
        >>> frame = tc.frame.import_csv(file_path, schema=schema, header=False)
        -etc-

        >>> frame.inspect()
        [#]  a  b  c
        ============
        [0]  à  ë  ñ
        [1]  ã  ê  ü

        </hide>

    """
    TkContext.validate(tc)
    require_type.non_empty_str(path, "path")
    require_type.non_empty_str(delimiter, "delimiter")
    require_type(bool, header, "header")
    require_type(str, datetime_format, "datetime_format")

    infer_schema = True
    column_names = []   # custom column names

    if schema is not None:
        if all(isinstance(item, basestring) for item in schema):
            # schema is just column names
            column_names = schema
            schema = None
        else:
            infer_schema = False   # if a custom schema is provided, don't waste time inferring the schema during load
            sparktk_schema.validate(schema)

    header_str = str(header).lower()
    infer_schema_str = str(infer_schema).lower()
    pyspark_schema = None

    if schema is not None:
        fields = []
        for column in schema:
            if dtypes._data_type_to_pyspark_type_table.has_key(column[1]):
                fields.append(StructField(column[0], dtypes._data_type_to_pyspark_type_table[column[1]], True))
            else:
                raise TypeError("Unsupported type {0} in schema for column {1}.".format(column[1], column[0]))
        pyspark_schema = StructType(fields)

    df = tc.sql_context.read.format(
        "com.databricks.spark.csv.org.trustedanalytics.sparktk").options(
            delimiter=delimiter,
            header=header_str,
            dateformat=datetime_format,
            inferschema=infer_schema_str).load(path, schema=pyspark_schema)

    df_schema = []

    if schema is None:
        for i, column in enumerate(df.schema.fields):
            try:
                datatype = dtypes.dtypes.get_primitive_type_from_pyspark_type(type(column.dataType))
            except ValueError:
                raise TypeError("Unsupported data type ({0}) for column {1}.".format(str(column.dataType), column.name))
            column_name = column_names[i] if (i < len(column_names)) else column.name
            df_schema.append((column_name, datatype))
    else:
        df_column_count = len(df.schema.fields)
        custom_column_count = len(schema)
        if (df_column_count != custom_column_count):
            raise ValueError("Bad schema value.  The number of columns in the custom schema ({0}) must match the"
                             "number of columns in the csv file data ({1}).".format(custom_column_count, df_column_count))
        df_schema = schema

    def cast_datetime(row):
        """
        The spark data frame gives uses datetime objects.  Convert them to long (ms since epoch) for our frame.
        """
        data = []
        for column_index in xrange(0, len(df_schema)):
            if df_schema[column_index][1] == dtypes.datetime and isinstance(row[column_index], datetime):
                data.append(long(dtypes.datetime_to_ms(row[column_index])))
            else:
                data.append(row[column_index])
        return data

    jrdd = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.rdd.PythonJavaRdd.scalaToPython(df._jdf.rdd())
    rdd = RDD(jrdd, tc.sc)

    if any(c[1] == dtypes.datetime for c in df_schema):
        # If any columns are date/time we must do this map
        rdd = df.rdd.map(cast_datetime)

    from sparktk.frame.frame import Frame  # circular dependency, so import late
    return Frame(tc, rdd, df_schema)
