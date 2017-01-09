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


def export_to_tensorflow(self, path):
    """
    Export frame to TensorFlow Records file on given path

    TensorFlow records are the standard data format for TensorFlow. The recommended format for TensorFlow is a TFRecords file
    containing tf.train.Example protocol buffers. The tf.train.Example protocol buffers encodes (which contain Features as a field).
    https://www.tensorflow.org/how_tos/reading_data

    During export, API parses Spark SQL DataTypes to TensorFlow compatible DataTypes as below

    * IntegerType or LongType =>  Int64List
    * FloatType or DoubleType => FloatList
    * ArrayType(Double) [Vector] => FloatList
    * Any other DataType (Ex: String) => BytesList

    Parameters
    ----------

    :param path: (str) HDFS/Local path to export current frame as TensorFlow records

    """
    self._scala.exportToTensorflow(path)
