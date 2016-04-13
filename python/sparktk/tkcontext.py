from lazyloader import get_lazy_loader
from sparktk.jvm.jutils import JUtils
from sparktk.sparkconf import create_sc
from sparktk.loggers import loggers
from pyspark import SparkContext


class TkContext(object):
    """TK Context - grounding object for the sparktk library"""

    def __init__(self, sc=None, **create_sc_kwargs):
        if not sc:
            sc = create_sc(**create_sc_kwargs)
        if type(sc) is not SparkContext:
            raise TypeError("sparktk context init requires a valid SparkContext.  Received type %s" % type(sc))
        self._sc = sc
        self._jtc = self._sc._jvm.org.trustedanalytics.at.TkContext(self._sc._jsc)
        self._jutils = JUtils(self._sc)
        loggers.set_spark(self._sc, "off")  # todo: undo this/move to config, I just want it outta my face most of the time

    @property
    def sc(self):
        return self._sc

    @property
    def jutils(self):
        return self._jutils

    @property
    def models(self):
        """access to the various models of sparktk"""
        return get_lazy_loader(self, "models")

    def to_frame(self, data, schema=None):
        """creates a frame from the given data"""
        from sparktk.frame.frame import to_frame
        return to_frame(self, data, schema)

    def load_frame(self, path):
        """loads a previously saved frame"""
        from sparktk.frame.frame import load_frame
        return load_frame(self, path)