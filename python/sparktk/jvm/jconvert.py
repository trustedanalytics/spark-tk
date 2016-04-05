from sparktk.dtypes import dtypes


class JConvert(object):
    """Handles misc. conversions going to and from Scala for simple non-primitive types, like lists, option, etc."""

    def __init__(self, jutils):
        self.jutils = jutils
        self.sc = jutils.sc
        self.scala = self.sc._jvm.org.trustedanalytics.at.jvm.JConvert

    def to_scala_list_double(self, python_list):
        return self.scala.toScalaListDouble([float(item) for item in python_list])

    def to_scala_list_string(self, python_list):
        return self.scala.toScalaListString([unicode(item) for item in python_list])

    def to_scala_vector_double(self, python_list):
        return self.scala.toScalaVectorDouble([float(item) for item in python_list])

    def to_scala_vector_string(self, python_list):
        return self.scala.toScalaVectorString([unicode(item) for item in python_list])

    def scala_map_string_int_to_python(self, m):
        return dict([(entry[0], int(entry[1])) for entry in list(self.scala.scalaMapStringIntToPython(m))])

    def scala_map_string_seq_to_python(self, m):
        return dict(self.scala.scalaMapStringSeqToPython(m))

    def to_scala_option(self, item):
        if item is None:
            return self.scala.noneOption()
        if self.jutils.is_java(item)  :
            return self.scala.toOption(item)
        if isinstance(item, basestring):
            return self.scala.someOptionString(item)
        if type(item) is long:
            return self.scala.someOptionLong(item)
        if dtypes.is_int(type(item)):
            return self.scala.someOptionInt(item)
        if dtypes.is_float(type(item)):
            return self.scala.someOptionDouble(item)
        if type(item) is list:
            return self.scala.someOptionList(item)
        raise NotImplementedError("Convert to scala Option[T] of type %s is not supported" % type(item))

    def from_scala_option(self, item):
        return self.scala.fromOption(item)

