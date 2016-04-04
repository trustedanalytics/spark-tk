from sparktk.jvm.jutils import JUtils
from sparktk.sparkconf import create_sc
from pyspark import SparkContext


class TkContext(object):
    """TK Context - grounding object for the sparktk library"""

    def __init__(self):
        self._sc = None
        self._jtc = None
        self._jutils = None

    def init(self, sc=None, **create_sc_kwargs):
        if not sc:
            sc = create_sc(**create_sc_kwargs)
        if type(sc) is not SparkContext:
            raise TypeError("sparktk context init requires a valid SparkContext.  Received type %s" % type(sc))
        self._sc = sc
        self._jtc = self._sc._jvm.org.trustedanalytics.at.TkContext(self._sc._jsc)
        self._jutils = JUtils(self._sc)
        # self.loggers.set_spark(self._sc, "off")  # todo: undo this/move to config, I just want it outta my face most of the time

    @property
    def sc(self):
        if not self._sc:
            raise TkContextInitError()
        return self._sc

    @property
    def jutils(self):
        if not self._sc:
            raise TkContextInitError()
        return self._jutils

    @property
    def jtc(self):
        if not self._sc:
            raise TkContextInitError()
        return self._jtc

    from sparktk.loggers import loggers
    from sparktk.frame.ops.inspect import inspect_settings

    def to_frame(tc, data, schema=None):
        from sparktk.frame.frame import to_frame
        return to_frame(tc, data, schema)

    def load_frame(tc, path):
        from sparktk.frame.frame import load_frame
        return load_frame(tc, path)


class TkContextInitError(RuntimeError):
    def __init__(self):
        RuntimeError.__init__(self, """TkContext is not initialized with a SparkContext.  Try running...
    >>> tc.init(sc)""")


