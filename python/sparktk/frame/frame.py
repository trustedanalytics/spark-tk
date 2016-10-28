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

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from sparktk.frame.pyframe import PythonFrame
from sparktk.frame.schema import schema_to_python, schema_to_scala, schema_is_coercible
from sparktk import dtypes
import logging
logger = logging.getLogger('sparktk')
from sparktk.propobj import PropertiesObject
from sparktk import TkContext

# import constructors for the API's sake (not actually dependencies of the Frame class)
from sparktk.frame.constructors.create import create
from sparktk.frame.constructors.import_csv import import_csv
from sparktk.frame.constructors.import_hbase import import_hbase
from sparktk.frame.constructors.import_hive import import_hive
from sparktk.frame.constructors.import_jdbc import import_jdbc
from sparktk.frame.constructors.import_pandas import import_pandas

__all__ = ["create",
           "Frame",
           "import_csv",
           "import_hbase",
           "import_hive",
           "import_jdbc",
           "import_pandas",
           "load"]


class Frame(object):
    
    def __init__(self, tc, source, schema=None, validate_schema=False):
        """(Private constructor -- use tc.frame.create or other methods available from the TkContext)"""
        self._tc = tc
        if self._is_scala_frame(source):
            self._frame = source
        elif self._is_scala_rdd(source):
            scala_schema = schema_to_scala(tc.sc, schema)
            self._frame = self._create_scala_frame(tc.sc, source, scala_schema)
        elif self._is_scala_dataframe(source):
            self._frame = self._create_scala_frame_from_scala_dataframe(tc.sc, source)
        elif isinstance(source, DataFrame):
            self._frame = self._create_scala_frame_from_scala_dataframe(tc.sc, source._jdf)
        elif isinstance(source, PythonFrame):
            self._frame = source
        else:
            if not isinstance(source, RDD):
                if not isinstance(source, list) or (len(source) > 0 and any(not isinstance(row, (list, tuple)) for row in source)):
                    raise TypeError("Invalid data source.  The data parameter must be a 2-dimensional list (list of row data) or an RDD.")

                inferred_schema = False
                if isinstance(schema, list):
                    if all(isinstance(item, basestring) for item in schema):
                        # check if schema is just a list of column names (versus string and data type tuples)
                        schema = self._infer_schema(source, schema)
                        inferred_schema = True
                    elif not all(isinstance(item, tuple) and
                                  len(item) == 2 and
                                  isinstance(item[0], basestring) for item in schema):
                        raise TypeError("Invalid schema.  Expected a list of tuples (str, type) with the column name and data type, but received type %s." % type(schema))
                    # check for duplicate column names
                    column_names = [col[0] for col in schema]
                    duplicate_column_names = set([col for col in column_names if column_names.count(col) > 1])
                    if len(duplicate_column_names) > 0:
                        raise ValueError("Invalid schema, column names cannot be duplicated: %s" % ", ".join(duplicate_column_names))
                elif schema is None:
                    schema = self._infer_schema(source)
                    inferred_schema = True
                else:
                    # Schema is not a list or None
                    raise TypeError("Invalid schema type: %s.  Expected a list of tuples (str, type) with the column name and data type." % type(schema))
                for item in schema:
                    if not self._is_supported_datatype(item[1]):
                        if inferred_schema:
                            raise TypeError("The %s data type was found when inferring the schema, and it is not a "
                                            "supported data type.  Instead, specify a schema that uses a supported data "
                                            "type, and enable validate_schema so that the data is converted to the proper "
                                            "data type.\n\nInferred schema: %s\n\nSupported data types: %s" %
                                            (str(item[1]), str(schema), dtypes.dtypes))
                        else:
                            raise TypeError("Invalid schema.  %s is not a supported data type.\n\nSupported data types: %s" %
                                            (str(item[1]), dtypes.dtypes))

                source = tc.sc.parallelize(source)
            if schema and validate_schema:
                # Validate schema by going through the data and checking the data type and attempting to parse it
                validate_schema_result = self.validate_pyrdd_schema(source, schema)
                source = validate_schema_result.validated_rdd
                logger.debug("%s values were unable to be parsed to the schema's data type." % validate_schema_result.bad_value_count)

            # If schema contains matrix datatype, then apply type_coercer to convert list[list] to numpy ndarray
            map_source = schema_is_coercible(source, list(schema))
            self._frame = PythonFrame(map_source, schema)

    def _merge_types(self, type_list_a, type_list_b):
        """
        Merges two lists of data types

        :param type_list_a: First list of data types to merge
        :param type_list_b: Second list of data types to merge
        :return: List of merged data types
        """
        if not isinstance(type_list_a, list) or not isinstance(type_list_b, list):
            raise TypeError("Unable to generate schema, because schema is not a list.")
        if len(type_list_a) != len(type_list_b):
            raise ValueError("Length of each row must be the same (found rows with lengths: %s and %s)." % (len(type_list_a), len(type_list_b)))
        return [dtypes._DataTypes.merge_types(type_list_a[i], type_list_b[i]) for i in xrange(0, len(type_list_a))]

    def _infer_types_for_row(self, row):
        """
        Returns a list of data types for the data in the specified row

        :param row: List or Row of data
        :return: List of data types
        """
        inferred_types = []
        for item in row:
            if item is None:
                inferred_types.append(int)
            elif not isinstance(item, list):
                inferred_types.append(type(item))
            else:
                inferred_types.append(dtypes.vector((len(item))))
        return inferred_types

    def _infer_schema(self, data, column_names=[], sample_size=100):
        """
        Infers the schema based on the data in the RDD.

        :param sc: Spark Context
        :param data: Data used to infer schema
        :param column_names: Optional column names to use in the schema.  If no column names are provided, columns
                             are given numbered names.  If there are more columns in the RDD than there are in the
                             column_names list, remaining columns will be numbered.
        :param sample_size: Number of rows to check when inferring the schema.  Defaults to 100.
        :return: Schema
        """
        inferred_schema = []

        if isinstance(data, list):
            if len(data) > 0:
                # get the schema for the first row
                data_types = self._infer_types_for_row(data[0])

                sample_size = min(sample_size, len(data))

                for i in xrange (1, sample_size):
                    data_types = self._merge_types(data_types, self._infer_types_for_row(data[i]))

                for i, data_type in enumerate(data_types):
                    column_name = "C%s" % i
                    if len(column_names) > i:
                        column_name = column_names[i]
                    inferred_schema.append((column_name, data_type))
        else:
            raise TypeError("Unable to infer schema, because the data provided is not a list.")
        return inferred_schema

    def _is_supported_datatype(self, data_type):
        """
        Returns True if the specified data_type is supported.
        """
        supported_primitives = [int, float, long, str, unicode]
        if data_type in supported_primitives:
            return True
        elif data_type is dtypes.datetime:
            return True
        elif type(data_type) is dtypes.vector:
            return True
        elif data_type is dtypes.matrix:
            return True
        else:
            return False

    def validate_pyrdd_schema(self, pyrdd, schema):
        if isinstance(pyrdd, RDD):
            schema_length = len(schema)
            num_bad_values = self._tc.sc.accumulator(0)

            def validate_schema(row, accumulator):
                data = []
                if len(row) != schema_length:
                    raise ValueError("Length of the row (%s) does not match the schema length (%s)." % (len(row), len(schema)))
                for index, column in enumerate(schema):
                    data_type = column[1]
                    try:
                        if row[index] is not None:
                            data.append(dtypes.dtypes.cast(row[index], data_type))
                    except:
                        data.append(None)
                        accumulator += 1
                return data

            validated_rdd = pyrdd.map(lambda row: validate_schema(row, num_bad_values))

            # Force rdd to load, so that we can get a bad value count
            validated_rdd.count()

            return SchemaValidationReturn(validated_rdd, num_bad_values.value)
        else:
            raise TypeError("Unable to validate schema, because the pyrdd provided is not an RDD.")

    @staticmethod
    def _create_scala_frame(sc, scala_rdd, scala_schema):
        """call constructor in JVM"""
        return sc._jvm.org.trustedanalytics.sparktk.frame.Frame(scala_rdd, scala_schema, False)

    @staticmethod
    def _create_scala_frame_from_scala_dataframe(sc, scala_dataframe):
        """call constructor in JVM"""
        return sc._jvm.org.trustedanalytics.sparktk.frame.Frame(scala_dataframe)

    @staticmethod
    def _from_scala(tc, scala_frame):
        """creates a python Frame for the given scala Frame"""
        return Frame(tc, scala_frame)

    def _frame_to_scala(self, python_frame):
        """converts a PythonFrame to a Scala Frame"""
        scala_schema = schema_to_scala(self._tc.sc, python_frame.schema)
        scala_rdd = self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.rdd.PythonJavaRdd.pythonToScala(python_frame.rdd._jrdd, scala_schema)
        return self._create_scala_frame(self._tc.sc, scala_rdd, scala_schema)

    def _is_scala_frame(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.Frame)

    def _is_scala_rdd(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self._tc.sc._jvm.org.apache.spark.rdd.RDD)

    def _is_scala_dataframe(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self._tc.sc._jvm.org.apache.spark.sql.DataFrame)

    def _is_python_rdd(self, item):
        return isinstance(item, RDD)

    @property
    def _is_scala(self):
        """answers whether the current frame is backed by a Scala Frame"""
        return self._is_scala_frame(self._frame)

    @property
    def _is_python(self):
        """answers whether the current frame is backed by a _PythonFrame"""
        return not self._is_scala

    @property
    def _scala(self):
        """gets frame backend as Scala Frame, causes conversion if it is current not"""
        if self._is_python:
            # If schema contains matrix dataype,
            # then apply type_coercer_pymlib to convert ndarray to pymlib DenseMatrix for serialization purpose at java
            self._frame.rdd = schema_is_coercible(self._frame.rdd, list(self._frame.schema), True)
            # convert PythonFrame to a Scala Frame"""
            scala_schema = schema_to_scala(self._tc.sc, self._frame.schema)
            scala_rdd = self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.rdd.PythonJavaRdd.pythonToScala(self._frame.rdd._jrdd, scala_schema)
            self._frame = self._create_scala_frame(self._tc.sc, scala_rdd, scala_schema)
        return self._frame

    @property
    def _python(self):
        """gets frame backend as _PythonFrame, causes conversion if it is current not"""
        if self._is_scala:
            # convert Scala Frame to a PythonFrame"""
            scala_schema = self._frame.schema()
            java_rdd =  self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.rdd.PythonJavaRdd.scalaToPython(self._frame.rdd())
            python_schema = schema_to_python(self._tc.sc, scala_schema)
            python_rdd = RDD(java_rdd, self._tc.sc)
            # If schema contains matrix datatype, then apply type_coercer to convert list[list] to numpy ndarray
            map_python_rdd = schema_is_coercible(python_rdd, list(python_schema))
            self._frame = PythonFrame(map_python_rdd, python_schema)
        return self._frame

    ##########################################################################
    # API
    ##########################################################################

    @property
    def rdd(self):
        """pyspark RDD  (causes conversion if currently backed by a Scala RDD)"""
        return self._python.rdd

    @property
    def dataframe(self):
        """pyspark DataFrame (causes conversion through Scala)"""
        return DataFrame(self._scala.dataframe(), self._tc.sql_context)

    @property
    def schema(self):
        if self._is_scala:
            return schema_to_python(self._tc.sc, self._frame.schema())  # need ()'s on schema because it's a def in scala
        return self._frame.schema

    @property
    def column_names(self):
        """
        Column identifications in the current frame.

        :return: list of names of all the frame's columns

        Returns the names of the columns of the current frame.

        Examples
        --------

            <skip>
            >>> frame.column_names
            [u'name', u'age', u'tenure', u'phone']

            </skip>

        """
        return [name for name, data_type in self.schema]

    # Frame Operations

    from sparktk.frame.ops.add_columns import add_columns
    from sparktk.frame.ops.append import append
    from sparktk.frame.ops.assign_sample import assign_sample
    from sparktk.frame.ops.bin_column import bin_column
    from sparktk.frame.ops.binary_classification_metrics import binary_classification_metrics
    from sparktk.frame.ops.box_cox import box_cox
    from sparktk.frame.ops.categorical_summary import categorical_summary
    from sparktk.frame.ops.collect import collect
    from sparktk.frame.ops.column_median import column_median
    from sparktk.frame.ops.column_mode import column_mode
    from sparktk.frame.ops.column_summary_statistics import column_summary_statistics
    from sparktk.frame.ops.copy import copy
    from sparktk.frame.ops.correlation import correlation
    from sparktk.frame.ops.correlation_matrix import correlation_matrix
    from sparktk.frame.ops.count import count
    from sparktk.frame.ops.covariance import covariance
    from sparktk.frame.ops.covariance_matrix import covariance_matrix
    from sparktk.frame.ops.cumulative_percent import cumulative_percent
    from sparktk.frame.ops.cumulative_sum import cumulative_sum
    from sparktk.frame.ops.dot_product import dot_product
    from sparktk.frame.ops.drop_columns import drop_columns
    from sparktk.frame.ops.drop_duplicates import drop_duplicates
    from sparktk.frame.ops.drop_rows import drop_rows
    from sparktk.frame.ops.ecdf import ecdf
    from sparktk.frame.ops.entropy import entropy
    from sparktk.frame.ops.export_data import export_to_csv, export_to_jdbc, export_to_json, export_to_hbase, export_to_hive
    from sparktk.frame.ops.filter import filter
    from sparktk.frame.ops.flatten_columns import flatten_columns
    from sparktk.frame.ops.group_by import group_by
    from sparktk.frame.ops.histogram import histogram
    from sparktk.frame.ops.inspect import inspect
    from sparktk.frame.ops.join_inner import join_inner
    from sparktk.frame.ops.join_left import join_left
    from sparktk.frame.ops.join_right import join_right
    from sparktk.frame.ops.join_outer import join_outer
    from sparktk.frame.ops.map_columns import map_columns
    from sparktk.frame.ops.matrix_covariance_matrix import matrix_covariance_matrix
    from sparktk.frame.ops.matrix_pca import matrix_pca
    from sparktk.frame.ops.matrix_svd import matrix_svd
    from sparktk.frame.ops.multiclass_classification_metrics import multiclass_classification_metrics
    from sparktk.frame.ops.power_iteration_clustering import power_iteration_clustering
    from sparktk.frame.ops.quantile_bin_column import quantile_bin_column
    from sparktk.frame.ops.quantiles import quantiles
    from sparktk.frame.ops.rename_columns import rename_columns
    from sparktk.frame.ops.reverse_box_cox import reverse_box_cox
    from sparktk.frame.ops.save import save
    from sparktk.frame.ops.sort import sort
    from sparktk.frame.ops.sortedk import sorted_k
    from sparktk.frame.ops.take import take
    from sparktk.frame.ops.tally import tally
    from sparktk.frame.ops.tally_percent import tally_percent
    from sparktk.frame.ops.timeseries_augmented_dickey_fuller_test import timeseries_augmented_dickey_fuller_test
    from sparktk.frame.ops.timeseries_breusch_godfrey_test import timeseries_breusch_godfrey_test
    from sparktk.frame.ops.timeseries_breusch_pagan_test import timeseries_breusch_pagan_test
    from sparktk.frame.ops.timeseries_durbin_watson_test import timeseries_durbin_watson_test
    from sparktk.frame.ops.timeseries_from_observations import timeseries_from_observations
    from sparktk.frame.ops.timeseries_slice import timeseries_slice
    from sparktk.frame.ops.to_pandas import to_pandas
    from sparktk.frame.ops.topk import top_k
    from sparktk.frame.ops.unflatten_columns import unflatten_columns


def load(path, tc=TkContext.implicit):
    """load Frame from given path"""
    TkContext.validate(tc)
    return tc.load(path, Frame)


class SchemaValidationReturn(PropertiesObject):
    """
    Return value from schema validation that includes the rdd of validated values and the number of bad values
    that were found.
    """

    def __init__(self, validated_rdd, bad_value_count):
        self._validated_rdd = validated_rdd
        self._bad_value_count = bad_value_count

    @property
    def validated_rdd(self):
        """
        RDD of values that have been casted to the data type specified by the frame's schema.
        """
        return self._validated_rdd

    @property
    def bad_value_count(self):
        """
        Number of values that were unable to be parsed to the data type specified by the schema.
        """
        return self._bad_value_count


