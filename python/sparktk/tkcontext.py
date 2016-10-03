from lazyloader import get_lazy_loader
from sparktk.jvm.jutils import JUtils
from sparktk.sparkconf import create_sc
from sparktk.loggers import loggers
from sparktk.arguments import require_type
from pyspark import SparkContext

import logging
logger = logging.getLogger('sparktk')

class TkContext(object):
    """TK Context - grounding object for the sparktk library"""
    _other_libs = None

    def __init__(self,
                 sc=None,
                 master=None,
                 py_files=None,
                 spark_home=None,
                 sparktk_home=None,
                 pyspark_submit_args=None,
                 app_name="sparktk",
                 other_libs=None,
                 extra_conf=None,
                 use_local_fs=False,
                 debug=None):
        """
        Creates a TkContext.

        If SparkContext sc is not provided, a new spark context will be created using the
        given settings and otherwise default values

        :param sc: (SparkContext) Active Spark Context, if not provided a new Spark Context is created with the
                   rest of the args
        :param master: (str) override spark master setting; for ex. 'local[4]' or 'yarn-client'
        :param py_files: (list) list of str of paths to python dependencies; Note the the current python
        package will be freshly zipped up and put in a tmp folder for shipping by spark, and then removed
        :param spark_home: (str) override $SPARK_HOME, the location of spark
        :param sparktk_home: (str) override $SPARKTK_HOME, the location of spark-tk
        :param pyspark_submit_args: (str) extra args passed to the pyspark submit
        :param app_name: (str) name of spark app that will be created
        :param other_libs: (list) other libraries (actual python packages or modules) that are compatible with spark-tk,
                           which need to be added to the spark context.  These libraries must be developed for use with
                           spark-tk and have particular methods implemented.  (See sparkconf.py _validate_other_libs)
        :param extra_conf: (dict) dict for any extra spark conf settings, for ex. {"spark.hadoop.fs.default.name": "file:///"}
        :param use_local_fs: (bool) simpler way to specify using local file system, rather than hdfs or other
        :param debug: (int or str) provide an port address to attach a debugger to the JVM that gets started
        :return: TkContext
        """
        if not sc:
            if SparkContext._active_spark_context:
                sc = SparkContext._active_spark_context
            else:
                sc = create_sc(master=master,
                               py_files=py_files,
                               spark_home=spark_home,
                               sparktk_home=sparktk_home,
                               pyspark_submit_args=pyspark_submit_args,
                               app_name=app_name,
                               other_libs=other_libs,
                               extra_conf=extra_conf,
                               use_local_fs=use_local_fs,
                               debug=debug)
        if type(sc) is not SparkContext:
            raise TypeError("sparktk context init requires a valid SparkContext.  Received type %s" % type(sc))
        self._sc = sc
        self._sql_context = None
        self._jtc = self._sc._jvm.org.trustedanalytics.sparktk.TkContext(self._sc._jsc)
        self._jutils = JUtils(self._sc)
        self._scala_sc = self._jutils.get_scala_sc()
        self._other_libs = other_libs if other_libs is None or isinstance(other_libs, list) else [other_libs]
        if self._other_libs is not None:
            for lib in self._other_libs:
                lib_obj = lib.get_main_object(self)
                setattr(self, lib.__name__, lib_obj)
        loggers.set_spark(self._sc, "off")  # todo: undo this/move to config, I just want it outta my face most of the time

    from sparktk.arguments import implicit

    @staticmethod
    def validate(tc, arg_name='tc'):
        """
        Raises a ValueError if the tc variable is not of type TkContext

        Since tc is so commonly used as an implicit variable, it's worth the special code here to save a lot of imports otherwise

        """
        require_type(TkContext, tc, arg_name)

    @property
    def sc(self):
        return self._sc

    @property
    def sql_context(self):
        if self._sql_context is None:
            from pyspark.sql import SQLContext
            self._sql_context = SQLContext(self.sc)
        return self._sql_context

    @property
    def jutils(self):
        return self._jutils

    @property
    def models(self):
        """access to the various models of sparktk"""
        return get_lazy_loader(self, "models", implicit_kwargs={'tc': self})

    @property
    def frame(self):
        return get_lazy_loader(self, "frame", implicit_kwargs={'tc': self}).frame  # .frame to account for extra 'frame' in name vis-a-vis scala

    @property
    def graph(self):
        return get_lazy_loader(self, "graph", implicit_kwargs={'tc': self}).graph  # .graph to account for extra 'graph' in name vis-a-vis scala

    @property
    def dicom(self):
        return get_lazy_loader(self, "dicom", implicit_kwargs={'tc': self}).dicom  # .dicom to account for extra 'dicom' in name vis-a-vis scala


    @property
    def examples(self):
        return get_lazy_loader(self, "examples", implicit_kwargs={'tc': self})

    def load(self, path, validate_type=None):
        """loads object from the given path (if validate_type is provided, error raised if loaded obj does not match"""
        loaders_map = None
        if self._other_libs is not None:
            other_loaders = []
            for other_lib in self._other_libs:
                other_loaders.append(other_lib.get_loaders(self))
            loaders_map =  self.jutils.convert.combine_scala_maps(other_loaders)
        scala_obj = self._jtc.load(path, self.jutils.convert.to_scala_option(loaders_map))
        python_obj = self._create_python_proxy(scala_obj)
        if validate_type and not isinstance(python_obj, validate_type):
          raise RuntimeError("load expected to get type %s but got type %s" % (validate_type, type(python_obj)))
        return python_obj

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
        try:
            relevant_path = ".".join(name_parts[name_parts.index('sparktk')+1:])
        except ValueError as e:
            # check if it's from another library
            relevant_path = ""
            for other_lib in self._other_libs:
                other_lib_name = other_lib.__name__
                if other_lib_name in name_parts:
                    relevant_path = ".".join(name_parts[name_parts.index(other_lib_name):])
                    break
            # if it's not from of the other libraries, then raise an error
            if relevant_path == "":
                raise ValueError("Trouble with class name %s, %s" % ('.'.join(name_parts), str(e)))
        cmd = "tc.%s._from_scala(tc, scala_obj)" % relevant_path
        logger.debug("tkcontext._create_python_proxy cmd=%s", cmd)
        proxy = eval(cmd, {"tc": self, "scala_obj": scala_obj})
        return proxy

    @property
    def agg(self):
        """access to the aggregation function enumeration"""
        from sparktk.frame.ops.group_by import agg
        return agg
