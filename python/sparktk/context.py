from sparktk.loggers import loggers
from sparktk.frame import Frame
#from sparktk.rdd import TkRDD
from py4j.java_gateway import is_instance_of
logger = loggers.set("debug", __name__)


class Context(object):
    """TK Context - grounding object for the sparktk library"""

    def __init__(self, spark_context):
        self.sc = spark_context
        self._jtk = self.sc._jvm.org.trustedanalytics.at.interfaces.TK(self.sc._jsc)
        self._loggers = loggers
        self._loggers.set_spark(self.sc, "off")  # todo: undo this/move to config, I just want it outta my face most of the time

    @property
    def loggers(self):
        """"""
        return self._loggers

    def to_frame(self, data, schema=None):
        # if self.is_python_rdd(item):
        #     return Frame(self, item, schema)
        # elif self.is_java(item):
        #     if self.is_scala_rdd(item):
        #         return Frame(self, TkRDD(item, schema, self.sc), schema)
        #     else:
        #         try:
        #             t = self.jtypestr(item)
        #         except:
        #             t = "<cannot determine>"
        #         raise TypeError("Cannot create frame from Java type %s" % t)
        # else:
        #     rdd = self.sc.parallelize(item)
        return Frame(self, data, schema)

    def is_java(self, item):
        import py4j
        return isinstance(item, py4j.java_gateway.JavaObject)

    def is_python_rdd(self, item):
        from pyspark.rdd import RDD
        return isinstance(item, RDD)

    def is_scala_rdd(self, item):
        return self.is_jvm_instance_of(item, self.sc._jvm.org.apache.spark.rdd.RDD)

    def is_jvm_instance_of(self, item, scala_type):
        if self.is_java(item):
            return is_instance_of(self.sc._gateway, item, scala_type)
        return False

    def jhelp(self, item):
        """shortcut to py4j's help method"""
        self.sc._gateway.help(item)

    def jtypestr(self, item):
        """string representation of the item's Java Type"""
        if self.is_java(item):
          return item.getClass().getName()
        return "<Not a JVM Object>"

