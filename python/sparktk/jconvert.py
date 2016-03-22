from sparktk.pyframe import PythonFrame
from sparktk.dtypes import dtypes
import pyspark

class JConvert(object):
    """Handles conversions going to and from Scala"""

    def __init__(self, sc):
        self.sc = sc
        self.scala = sc._jvm.org.trustedanalytics.at.jconvert.PythonConvert

    def create_scala_frame(self, scala_rdd, scala_schema):
        """call constructor in JVM"""
        return self.sc._jvm.org.trustedanalytics.at.interfaces.Frame(scala_rdd, scala_schema)

    def frame_to_python(self, scala_frame):
        """converts a Scala Frame to a PythonFrame"""
        scala_schema = scala_frame.schema()
        java_rdd = self.scala.scalaToPython(scala_frame.rdd(), scala_schema)
        python_schema = self.schema_to_python(scala_schema)
        # todo: figure out serialization such that we don't get tuples back, but lists, to avoid this full pass :(
        def to_list(row):
            return list(row)
        python_rdd = pyspark.rdd.RDD(java_rdd, self.sc).map(to_list)

        return PythonFrame(python_rdd, python_schema)

    def frame_to_scala(self, python_frame):
        """converts a PythonFrame to a Scala Frame"""
        scala_schema = self.schema_to_scala(python_frame.schema)
        scala_rdd = self._python_jrdd_to_scala_rdd(python_frame.rdd._jrdd, scala_schema)
        return self.create_scala_frame(scala_rdd, scala_schema)

    def schema_to_scala(self, python_scala):
        list_of_list_of_str_schema = map(lambda t: [t[0], dtypes.to_string(t[1])], python_scala)  # convert dtypes to strings
        return self.scala.frameSchemaToScala(list_of_list_of_str_schema)

    def schema_to_python(self, scala_schema):
        list_of_list_of_str_schema = self.scala.frameSchemaToPython(scala_schema)
        return [(name, dtypes.get_from_string(dtype)) for name, dtype in list_of_list_of_str_schema]

    def _scala_rdd_to_jrdd(self, srdd):
        """converts a Scala RDD serialized from Scala usage to a Java RDD serialized for Python RDD usage"""
        return self.scala.scalaToPython(srdd)

    def _python_jrdd_to_scala_rdd(self, jrdd, scala_schema):
        """converts a Java RDD serialized from Python RDD usage to a Scala RDD serialized for Scala RDD usage"""
        return self.scala.pythonToScala(jrdd, scala_schema)

    def list_to_scala_double(self, python_list):
        return self.scala.toScalaListDouble([float(item) for item in python_list])

    def list_to_scala_string(self, python_list):
        return self.scala.toScalaListString([unicode(item) for item in python_list])

    def to_scala_vector_double(self, python_list):
        return self.scala.toScalaVectorDouble([float(item) for item in python_list])

    def to_scala_vector_string(self, python_list):
        return self.scala.toScalaVectorString([unicode(item) for item in python_list])

    def scala_map_string_int_to_python(self, m):
        return dict([(entry[0], int(entry[1])) for entry in list(self.scala.scalaMapStringIntToPython(m))])

    def to_option(self, item):
        if item is None:
            return self.scala.noneOption()
        if isinstance(item, basestring):
            return self.scala.someOptionString(item)
        if dtypes.is_int(item):
            return self.scala.someOptionInt(item)
        if dtypes.is_float(item):
            return self.scala.someOptionDouble(item)
        raise NotImplementedError("Convert to scala Option[T] of type %s is not supported" % type(item))

    def from_option(self, item):
        return self.scala.fromOption(item)
