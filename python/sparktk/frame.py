from pyspark.rdd import RDD
from sparktk.dtypes import dtypes
from sparktk.pyframe import PythonFrame


class Frame(object):

    def __init__(self, context, source, schema=None):
        self._context = context
        if self._context.is_scala_frame(source):
            self._frame = source
        elif self._context.is_scala_rdd(source):
            scala_schema = self._context.jconvert.schema_to_scala(schema)
            self._frame = self._context.jconvert.create_scala_frame(source, scala_schema)
        else:
            if not isinstance(source, RDD):
                source = self._context.sc.parallelize(source)
            if schema:
                self.validate_pyrdd_schema(source, schema)
            self._frame = PythonFrame(source, schema)

    def validate_pyrdd_schema(self, pyrdd, schema):
        pass

    @property
    def _is_scala(self):
        """answers whether the current frame is backed by a Scala Frame (alternative is a _PythonFrame)"""
        return self._context.is_scala_frame(self._frame)

    @property
    def _scala(self):
        """gets frame backend as Scala Frame, causes conversion if it is current not"""
        if not self._is_scala:
            self._frame = self._context.jconvert.frame_to_scala(self._frame)
        return self._frame

    @property
    def _python(self):
        """gets frame backend as _PythonFrame, causes conversion if it is current not"""
        if self._is_scala:
            self._frame = self._context.jconvert.frame_to_python(self._frame)
        return self._frame

    def _get_scala_row_to_python_converter(self, schema):
        """gets a converter for going from scala value to python value, according to dtype"""
        row_schema = schema

        def to_dtype(value, dtype):
            try:
                return dtypes.cast(value, dtype)
            except:
                return None

        def scala_row_to_python(scala_row):
            num_cols = scala_row.length()
            return [to_dtype(scala_row.get(i), row_schema[i][1]) for i in xrange(num_cols)]
        return scala_row_to_python

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
            return self._context.jconvert.schema_to_python(self._frame.schema())  # need ()'s on schema because it's a def in scala
        return self._frame.schema

    def append_csv_file(self, file_name, schema, separator=','):
        self._scala.appendCsvFile(file_name, self._context.jconvert.schema_to_scala(schema), separator)

    def export_to_csv(self, file_name):
        self._scala.exportToCsv(file_name)

    def count(self):
        if self._is_scala:
            return int(self._scala.count())
        return self.rdd.count()

    # Frame Operations

    from sparktk.frameops.add_columns import add_columns
    from sparktk.frameops.bin_column import bin_column
    from sparktk.frameops.drop_rows import drop_rows
    from sparktk.frameops.filter import filter
    from sparktk.frameops.inspect import inspect
    from sparktk.frameops.save import save
    from sparktk.frameops.take import take



