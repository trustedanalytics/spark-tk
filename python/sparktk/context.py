from sparktk.loggers import loggers
from sparktk.frame.frame import Frame
from sparktk.jvm.jutils import JUtils

logger = loggers.set("debug", __name__)


class Context(object):
    """TK Context - grounding object for the sparktk library"""

    def __init__(self, spark_context):
        self.sc = spark_context
        self._jutils = JUtils(spark_context)
        self._jcontext = self.sc._jvm.org.trustedanalytics.at.context.Context(self.sc._jsc)
        self._loggers = loggers
        #self._loggers.set_spark(self.sc, "off")  # todo: undo this/move to config, I just want it outta my face most of the time

    @property
    def loggers(self):
        """"""
        return self._loggers

    @property
    def jutils(self):
        return self._jutils

    def to_frame(self, data, schema=None):
        return Frame(self, data, schema)

    def load_frame(self, path):
        return Frame(self, self._jcontext.loadFrame(path))



