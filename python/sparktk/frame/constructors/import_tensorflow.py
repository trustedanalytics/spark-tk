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

    :return: a frame
    """

    if schema is not None:
        scala_frame_schema = tc.jutils.convert.to_scala_frame_schema(schema)
    else:
        scala_frame_schema = schema

    scala_frame = tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.constructors.ImportTensorflow.importTensorflow(tc._scala_sc,
                                                                                                                        tf_path,
                                                                                                                        tc.jutils.convert.to_scala_option(scala_frame_schema))

    from sparktk.frame.frame import Frame
    return Frame(tc, scala_frame)