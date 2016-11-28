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

from sparktk.dtypes import _Matrix
from sparktk.dtypes import dtypes
from pyspark.mllib.linalg import DenseMatrix


def jvm_scala_schema(sc):
    return sc._jvm.org.trustedanalytics.sparktk.frame.SchemaHelper


def get_schema_for_columns(schema, selected_columns):
    indices = get_indices_for_selected_columns(schema, selected_columns)
    return [schema[i] for i in indices]


def get_indices_for_selected_columns(schema, selected_columns):
    indices = []
    schema_columns = [col[0] for col in schema]
    for column in selected_columns:
        try:
            indices.append(schema_columns.index(column))
        except:
            raise ValueError("Invalid column name %s provided"
                             ", please choose from: (%s)" % (column, ",".join(schema_columns)))

    return indices


def schema_to_scala(sc, python_schema):
    list_of_list_of_str_schema = map(lambda t: [t[0], dtypes.to_string(t[1])], python_schema)  # convert dtypes to strings
    return jvm_scala_schema(sc).pythonToScala(list_of_list_of_str_schema)


def schema_to_python(sc, scala_schema):
    list_of_list_of_str_schema = jvm_scala_schema(sc).scalaToPython(scala_schema)
    return [(name, dtypes.get_from_string(dtype)) for name, dtype in list_of_list_of_str_schema]


def validate(python_schema):
    """
    Raises an error if the given schema is not a tuple or list of tuples of the type (column_name, data type),
    where column_name must be a string followed by supported data type.
    """
    if not isinstance(python_schema, list):
        python_schema = [python_schema]
    unique_names = set()
    for item in python_schema:
        if not isinstance(item, tuple):
            raise ValueError("schema expected to contain tuples, encountered type %s" % type(item))
        if not isinstance(item[0], basestring):
            raise ValueError("first entry in schema tuple should be a string, received type %s: %s" %
                             (type(item[0]), str(item[0])))
        if len(item) != 2:
            raise ValueError("schema tuples should have 2 items (column name and type), but found tuple with length: %s" %
                             len(item))
        unique_names.add(item[0])
    if len(unique_names) != len(python_schema):
        names = map(lambda x: x[0], python_schema)
        for u in unique_names:
            names.remove(u)
        raise ValueError("schema has duplicate column names: %s" % str(names))


def validate_is_mergeable(tc, *python_schema):
    """
    Raises an error if the column names in the given schema conflict
    """

    scala_schema_list = []
    for schema in python_schema:
        if not isinstance(schema, list):
            schema = [schema]
        scala_schema_list.append(schema_to_scala(tc.sc, schema))

    jvm_scala_schema(tc.sc).validateIsMergeable(tc.jutils.convert.to_scala_list(scala_schema_list))


def schema_is_coercible(source, python_schema, in_scala=False):
    """
    check whether python schema is coercible or not.
    Like if schema contains matrix datatype, convert list[list] to numpy ndarray
    """
    flag = False
    for schema in python_schema:
        if type(schema[1]) == _Matrix:
            flag = True
            break

    if flag:
        if in_scala:
            map_source = source.map(type_coercer_pymllib(python_schema))
        else:
            map_source = source.map(type_coercer(python_schema))
    else:
        map_source = source

    return map_source


def type_coercer(schema):
    """
    When creating a new frame(python frame created) or converting frame from scala to python frame,
    the function scans a row and performs below
        * when creating a new frame(python frame created) - if it finds list[list](which represents matrix) as column value,
          converts it to numpy ndarray
        * when Converting frame from scala to python frame - (scala converts DenseMatrix--> JList[JList[Double]](in JConvert.scala),
          jconvert.py converts JList[JList[Double]] --> list[list[float]]), converts list[list] to ndarray

    """
    matrix_indices = [i for i in xrange(len(schema)) if type(schema[i][1]) == _Matrix]

    def decorator(row):
        import numpy as np
        for i in matrix_indices:
            if isinstance(row[i], list):
                row[i] = np.array(row[i], dtype=np.float64)
        return row
    return decorator


def type_coercer_pymllib(schema):
    """
    When converting from python to scala, function scans the row and converts the ndarray
    to python mllib DenseMatrix. so that autopicklers understands how to serialize from pyspark mllib DenseMatrix to Scala MLlib DenseMatrix.
    For Serialization to work we have to explicitly call SparkAliases.MLLibSerDe which initializes/registers all required classes
    (DenseVectorPickler, DenseMatrixPickler, SparseMatrixPickler, SparseVectorPickler) in pythonToScala() method of PythonJavaRdd.scala class

    ndarray stores data as row-major where as mllib densematrix stores data as column-major.
    To construct mllib DenseMatrix with row-major we are transposing ndarray to maintain consistency.
    """
    matrix_indices = [i for i in xrange(len(schema)) if type(schema[i][1]) == _Matrix]

    def decorator(row):
        for i in matrix_indices:
            shape = row[i].shape
            # By default Mllib DenseMatrix constructs column-major matrix.
            # So Transposing ndarray to maintain consistency
            arr = row[i].transpose().flatten()
            row[i] = DenseMatrix(shape[0], shape[1], arr)
        return row
    return decorator

