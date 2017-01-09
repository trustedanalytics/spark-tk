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

from sparktk import TkContext
from sparktk.frame.schema import schema_to_scala


def import_tensorflow(tf_path, schema=None, tc=TkContext.implicit):
    """
    Create a frame with data from a TensorFlow records file

    TensorFlow records are the standard data format for TensorFlow. The recommended format for TensorFlow is a TFRecords file
    containing tf.train.Example protocol buffers. The tf.train.Example protocol buffers encodes (which contain Features as a field).
    https://www.tensorflow.org/how_tos/reading_data

    During Import, API parses TensorFlow DataTypes as below

    * Int64List => IntegerType or LongType
    * FloatList => FloatType or DoubleType
    * Any other DataType (Ex: String) => BytesList

    Parameters
    ----------

    :param tf_path:(str) Full path to TensorFlow records
    :param schema: (Optional(list[tuple(str, type)] or list[str])) The are different options for specifying a schema:

    * Provide the full schema for the frame as a list of tuples (string column name and data type)
    * Provide the column names as a list of strings.  Column data types will be inferred, based on the data.

    :return: frame with data from TensorFlow records

    Examples
    --------

        >>> file_path = "../datasets/cities.csv"

        >>> frame = tc.frame.import_csv(file_path, "|", header=True)
        -etc-

        >>> frame.count()
        20

        >>> frame.sort("rank")

        >>> frame.inspect()
        [#]  rank  city         population_2013  population_2010  change  county
        ============================================================================
        [0]     1  Portland              609456           583776  4.40%   Multnomah
        [1]     2  Salem                 160614           154637  3.87%   Marion
        [2]     3  Eugene                159190           156185  1.92%   Lane
        [3]     4  Gresham               109397           105594  3.60%   Multnomah
        [4]     5  Hillsboro              97368            91611  6.28%   Washington
        [5]     6  Beaverton              93542            89803  4.16%   Washington
        [6]     7  Bend                   81236            76639  6.00%   Deschutes
        [7]     8  Medford                77677            74907  3.70%   Jackson
        [8]     9  Springfield            60177            59403  1.30%   Lane
        [9]    10  Corvallis              55298            54462  1.54%   Benton

        >>> destPath = "../tests/sandbox/output26.tfr"

        >>> import os

        >>> os.remove(destPath)

        >>> frame.export_to_tensorflow(destPath)

        >>> tf_schema=[("rank", int),("city", unicode),("population_2013", int),("population_2010", int),("change", unicode),("county", unicode)]

        >>> tf_frame = tc.frame.import_tensorflow(destPath, tf_schema)

        >>> tf_frame.count()
        20

        >>> tf_frame.sort("rank")

        >>> tf_frame.inspect()
        [#]  rank  city         population_2013  population_2010  change  county
        ============================================================================
        [0]     1  Portland              609456           583776  4.40%   Multnomah
        [1]     2  Salem                 160614           154637  3.87%   Marion
        [2]     3  Eugene                159190           156185  1.92%   Lane
        [3]     4  Gresham               109397           105594  3.60%   Multnomah
        [4]     5  Hillsboro              97368            91611  6.28%   Washington
        [5]     6  Beaverton              93542            89803  4.16%   Washington
        [6]     7  Bend                   81236            76639  6.00%   Deschutes
        [7]     8  Medford                77677            74907  3.70%   Jackson
        [8]     9  Springfield            60177            59403  1.30%   Lane
        [9]    10  Corvallis              55298            54462  1.54%   Benton

    """

    if schema is not None:
        schema_as_list_of_lists = [list(elem) for elem in schema]
        scala_frame_schema = schema_to_scala(schema_as_list_of_lists)
    else:
        scala_frame_schema = schema

    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.constructors.ImportTensorflow.importTensorflow(tc._scala_sc,
                                                                                                                        tf_path,
                                                                                                                        tc.jutils.convert.to_scala_option(scala_frame_schema))
    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)