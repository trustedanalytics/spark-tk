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

    During export, the API parses Spark SQL DataTypes to TensorFlow compatible DataTypes as below:

    * IntegerType or LongType =>  Int64List
    * FloatType or DoubleType => FloatList
    * ArrayType(Double) [Vector] => FloatList
    * Any other DataType (Ex: String) => BytesList

    Parameters
    ----------

    :param path: (str) HDFS/Local path to export current frame as TensorFlow records


    Examples
    --------

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
        [6]     7  Bend                   81236            76639  6.00%   Deschutes
        [7]     8  Medford                77677            74907  3.70%   Jackson
        [8]     9  Springfield            60177            59403  1.30%   Lane
        [9]    10  Corvallis              55298            54462  1.54%   Benton

        >>> destPath = "../tests/sandbox/output24.tfr"

        >>> import os
        ... if os.path.exists(filename) os.remove(destPath)

        >>> frame.export_to_tensorflow(destPath)

        Check for output24.tfr in specified destination path either on Local or HDFS file system

    """
    self._scala.exportToTensorflow(path)
