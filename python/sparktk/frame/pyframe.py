class PythonFrame(object):
    """frame backend using a Python objects: pyspark.rdd.RDD, [(str, dtype), (str, dtype), ...]"""

    def __init__(self, rdd, schema=None):
        self.rdd = rdd
        self.schema = schema
