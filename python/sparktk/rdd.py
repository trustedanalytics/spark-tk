from pyspark.rdd import RDD
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer
from sparktk.dtypes import schema_to_scala


def _srdd_to_jrdd(srdd, sc):
    """converts a Scala RDD serialized from Scala usage to a Java RDD serialized for Python RDD usage"""
    return sc._jvm.org.trustedanalytics.at.interfaces.PythonSerialization.scalaToPython(srdd)


def _jrdd_to_srdd(jrdd, schema, sc):
    """converts a Java RDD serialized from Python RDD usage to a Scala RDD serialized for Scala RDD usage"""
    return sc._jvm.org.trustedanalytics.at.interfaces.PythonSerialization.pythonToScala(jrdd, schema_to_scala(schema, sc))


class TkRDD(RDD):
    """
    represents a Scala RDD in JVM which has not been formatted (serialized) for python.rdd.RDD

    These RDDs are what the core spark-tk scala library works with.  Rows must be serialized
    to arrays of bytes before the pyspark code will work on them.
    """

    # Could be pitfalls in here....

    def __init__(self, scala_rdd, schema, ctx, jrdd_deserializer=AutoBatchedSerializer(PickleSerializer())):
        # intentionally not calling super; this function mimics pyspark RDD's __init__
        if scala_rdd is None:
            raise ValueError("srdd argument cannot be None")
        self._srdd_value = scala_rdd
        self._jrdd_value = None
        self._schema = schema
        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = ctx
        self._jrdd_deserializer = jrdd_deserializer
        self._id = scala_rdd.id()
        self.partitioner = None

    @property
    def _jrdd(self):
        """Override to create the appropriate Java RDD backing expected by pyspark's RDD when accessing _jrdd"""
        if self._jrdd_value is None:
            print "Converting Scala serialization to Python serialization"
            # convert this RDD proxy from a scala RDD reference to pyspark RDD reference
            if self._srdd_value is None:
                raise ValueError("Internal error: self._srdd is None")
            jrdd = _srdd_to_jrdd(self._srdd_value, self.ctx)
            pyspark_rdd = RDD(jrdd, self.ctx)
            pyspark_rdd.count()  # force action in order to debug
            print "Python serialization deemed succesful"
            self._jrdd_value = pyspark_rdd._jrdd
            self._srdd_value = None
        return self._jrdd_value

    @property
    def _srdd(self):
        """Gets the Scala RDD[Row] reference, converting a JavaRDD[Array[Byte]] if necessary"""
        if self._srdd_value is None:
            if self._jrdd_value is None:
                raise ValueError("Internal error: self._pyspark_jrdd is None")
            self._srdd_value = _jrdd_to_srdd(self._jrdd_value, self._schema, self.ctx)
            self._jrdd_value = None
        return self._srdd_value

    def id(self):   # todo: there maybe some quirks with id, haven't explored it yet; best guess here
        if self._id is None:
            self._id = self._srdd_value.id()
        return self._id

    def _is_pipelinable(self):
        return not (self.is_cached or self.is_checkpointed)

    @classmethod
    def from_python(cls, python_rdd, schema, sc):
        """Factory method for (sparktk) ObjectScalaRDD, from (pyspark) RDD"""
        srdd = _jrdd_to_srdd(python_rdd._jrdd, schema, sc)
        return cls(srdd, schema, sc)
