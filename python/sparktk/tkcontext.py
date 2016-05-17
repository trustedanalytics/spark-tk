from lazyloader import get_lazy_loader
from sparktk.jvm.jutils import JUtils
from sparktk.sparkconf import create_sc
from sparktk.loggers import loggers
from pyspark import SparkContext
import logging
logger = logging.getLogger('sparktk')

class TkContext(object):
    """TK Context - grounding object for the sparktk library"""

    def __init__(self, sc=None, **create_sc_kwargs):
        if not sc:
            sc = create_sc(**create_sc_kwargs)
        if type(sc) is not SparkContext:
            raise TypeError("sparktk context init requires a valid SparkContext.  Received type %s" % type(sc))
        self._sc = sc
        self._jtc = self._sc._jvm.org.trustedanalytics.sparktk.TkContext(self._sc._jsc)
        self._jutils = JUtils(self._sc)
        self._scala_sc = self._jutils.get_scala_sc()
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

    @property
    def frame(self):
        return get_lazy_loader(self, "frame").frame  # .frame to account for extra 'frame' is name vis-a-vis scala

    def to_frame(self, data, schema=None):
        """creates a frame from the given data"""
        from sparktk.frame.frame import Frame
        return Frame.to_frame(self, data, schema)

    def load(self, path):
        """loads an object from the given path"""
        scala_obj = self._jtc.load(path)
        return self._create_python_proxy(scala_obj)

    def _create_python_proxy(self, scala_obj):
        """Create a python object for the scala_obj

        Convention is such that the python proxy object is available off the TkContext with the SAME
        path that the object has in Scala, starting with sparktk.

        Example:

        org.trustedanalytics.sparktk.models.clustering.kmeans.KMeansModel

        means a call to

        tc.models.clustering.kmeans.KMeansModel.load(tc, scala_obj)

        The signature is simply the python tc and the reference to the scala obj
        """
        name_parts = scala_obj.getClass().getName().split('.')
        relevant_path = ".".join(name_parts[name_parts.index('sparktk')+1:])
        cmd = "tc.%s.load(tc, scala_obj)" % relevant_path
        logger.debug("tkcontext._create_python_proxy cmd=%s", cmd)
        proxy = eval(cmd, {"tc": self, "scala_obj": scala_obj})
        return proxy
