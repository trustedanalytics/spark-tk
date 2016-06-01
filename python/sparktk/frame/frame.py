from pyspark.rdd import RDD

from sparktk.frame.pyframe import PythonFrame
from sparktk.frame.schema import schema_to_python, schema_to_scala


# import constructors for the API's sake (not actually dependencies of the Frame class)
from sparktk.frame.constructors.create import create
from sparktk.frame.constructors.import_csv import import_csv


class Frame(object):

    def __init__(self, tc, source, schema=None):
        self._tc = tc
        if self._is_scala_frame(source):
            self._frame = source
        elif self.is_scala_rdd(source):
            scala_schema = schema_to_scala(tc.sc, schema)
            self._frame = self.create_scala_frame(tc.sc, source, scala_schema)
        else:
            if not isinstance(source, RDD):
                source = tc.sc.parallelize(source)
            if schema:
                self.validate_pyrdd_schema(source, schema)
            self._frame = PythonFrame(source, schema)

    def validate_pyrdd_schema(self, pyrdd, schema):
        pass

    @staticmethod
    def create_scala_frame(sc, scala_rdd, scala_schema):
        """call constructor in JVM"""
        return sc._jvm.org.trustedanalytics.sparktk.frame.Frame(scala_rdd, scala_schema)

    @staticmethod
    def load(tc, scala_frame):
        """creates a python Frame for the given scala Frame"""
        return Frame(tc, scala_frame)

    def _frame_to_scala(self, python_frame):
        """converts a PythonFrame to a Scala Frame"""
        scala_schema = schema_to_scala(self._tc.sc, python_frame.schema)
        scala_rdd = self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.rdd.PythonJavaRdd.pythonToScala(python_frame.rdd._jrdd, scala_schema)
        return self.create_scala_frame(self._tc.sc, scala_rdd, scala_schema)

    def _is_scala_frame(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.Frame)

    def is_scala_rdd(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self._tc.sc._jvm.org.apache.spark.rdd.RDD)

    def is_python_rdd(self, item):
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
            # convert PythonFrame to a Scala Frame"""
            scala_schema = schema_to_scala(self._tc.sc, self._frame.schema)
            scala_rdd = self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.rdd.PythonJavaRdd.pythonToScala(self._frame.rdd._jrdd, scala_schema)
            self._frame = self.create_scala_frame(self._tc.sc, scala_rdd, scala_schema)
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
            self._frame = PythonFrame(python_rdd, python_schema)
        return self._frame

    ##########################################################################
    # API
    ##########################################################################

    @property
    def rdd(self):
        """pyspark RDD  (causes conversion if currently backed by a Scala RDD)"""
        return self._python.rdd

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

        .. code::

            >>> frame.column_names
            [u'name', u'age', u'tenure', u'phone']

        """
        return [name for name, data_type in self.schema]

    @property
    def row_count(self):
        """
        Number of rows in the current frame.

        :return: The number of rows in the frame

        Counts all of the rows in the frame.

        Examples
        --------
        Get the number of rows:

        <hide>
        frame = tc.frame.create([[item] for item in range(0, 4)],[("a", int)])
        </hide>

        .. code::

            >>> frame.row_count
            4

        """
        if self._is_scala:
            return int(self._scala.rowCount())
        return self.rdd.count()

    def append_csv_file(self, file_name, schema, separator=','):
        self._scala.appendCsvFile(file_name, schema_to_scala(self._tc.sc, schema), separator)

    def export_to_csv(self, file_name):
        self._scala.exportToCsv(file_name)

    # Frame Operations

    from sparktk.frame.ops.add_columns import add_columns
    from sparktk.frame.ops.append import append
    from sparktk.frame.ops.assign_sample import assign_sample
    from sparktk.frame.ops.bin_column import bin_column
    from sparktk.frame.ops.binary_classification_metrics import binary_classification_metrics
    from sparktk.frame.ops.categorical_summary import categorical_summary
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
    from sparktk.frame.ops.export_data import export_to_jdbc, export_to_json, export_to_hbase, export_to_hive
    from sparktk.frame.ops.filter import filter
    from sparktk.frame.ops.flatten_columns import flatten_columns
    from sparktk.frame.ops.histogram import histogram
    from sparktk.frame.ops.inspect import inspect
    from sparktk.frame.ops.multiclass_classification_metrics import multiclass_classification_metrics
    from sparktk.frame.ops.quantile_bin_column import quantile_bin_column
    from sparktk.frame.ops.quantiles import quantiles
    from sparktk.frame.ops.rename_columns import rename_columns
    from sparktk.frame.ops.save import save
    from sparktk.frame.ops.sort import sort
    from sparktk.frame.ops.sortedk import sorted_k
    from sparktk.frame.ops.take import take
    from sparktk.frame.ops.tally import tally
    from sparktk.frame.ops.tally_percent import tally_percent
    from sparktk.frame.ops.topk import top_k
    from sparktk.frame.ops.unflatten_columns import unflatten_columns
