from pyspark.rdd import RDD
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer


class ObjectScalaRDD(RDD):
    """
    represents a Scala RDD in JVM which has not been formatted (serialized) for python.rdd.RDD

    These RDDs are what the core spark-tk scala library works with.  Rows must be serialized
    to arrays of bytes before the pyspark code will work on them.
    """

    # Could be pitfalls in here....

    def __init__(self, scala_rdd, ctx, jrdd_deserializer=AutoBatchedSerializer(PickleSerializer())):
        # intentionally not calling super; this function mimics pyspark RDD's __init__
        if scala_rdd is None:
            raise ValueError("scala_rdd argument cannot be None")
        self._scala_rdd = scala_rdd
        self._pyspark_jrdd = None
        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = ctx
        self._jrdd_deserializer = jrdd_deserializer
        self._id = scala_rdd.id()
        self.partitioner = None

    @property
    def _jrdd(self):
        """Override to create the appropriate Java RDD backing expected by pyspark's RDD when accessing _jrdd"""
        if self._pyspark_jrdd is None:
            # convert from scala RDD reference to pyspark RDD reference (one-way)
            java_rdd = self._scala_rdd.toJavaRDD()
            pyspark_ready_java_rdd = self._convert_to_pyspark_serialization(java_rdd)
            pyspark_rdd = RDD(pyspark_ready_java_rdd, self.ctx)
            self._pyspark_jrdd = pyspark_rdd._jrdd
            self._scala_rdd = None
        return self._pyspark_jrdd

    def id(self):
        if self._id is None:
            self._id = self._scala_rdd.id()
        return self._id

    def _is_pipelinable(self):
        return not (self.is_cached or self.is_checkpointed)

    def _convert_to_pyspark_serialization(self, jrdd):
        """converts a Java RDD serialized from Scala usage to a Java RDD serialized for Python RDD usage"""
        return self.ctx._jvm.org.trustedanalytics.at.interfaces.PythonSerialization.javaToPython(jrdd)

    def _convert_from_pyspark_serialization(self, jrdd):
        """converts a Java RDD serialized from Python RDD usage to a Java RDD serialized for Scala RDD usage"""
        return self.ctx._jvm.org.trustedanalytics.at.interfaces.PythonSerialization.pythonToJava(jrdd)
