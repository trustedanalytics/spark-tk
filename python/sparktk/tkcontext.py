# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from lazyloader import get_lazy_loader
from sparktk.jvm.jutils import JUtils
from sparktk.sparkconf import create_sc, default_spark_master
from sparktk.loggers import loggers
from sparktk.arguments import require_type
from pyspark import SparkContext

import logging
logger = logging.getLogger('sparktk')


__all__ = ['TkContext']


class TkContext(object):
    """
    TK Context

    The sparktk Python API centers around the TkContext object.  This object holds the session's requisite
    SparkContext object in order to work with Spark.  It also provides the entry point to the main APIs.
    """

    _other_libs = None
    __mock = object()

    def __init__(self,
                 sc=None,
                 master=None,
                 py_files=None,
                 spark_home=None,
                 sparktk_home=None,
                 pyspark_submit_args=None,
                 app_name=None,
                 other_libs=None,
                 extra_conf_file=None,
                 extra_conf_dict=None,
                 use_local_fs=False,
                 debug=None):
        r"""
        Creates a TkContext object

        :param sc: (SparkContext) Active Spark Context, if not provided a new Spark Context is created with the
                   rest of the args
                   (see https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkContext.html)
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
        :param extra_conf_file: (str) local file path to a spark conf file to supplement the spark conf
                                File format is basic key-value pairs per line, like:

                                    spark.executor.memory=6g
                                    spark.files.overwrite=true

        (NOTE: if env var $SPARKTK_EXTRA_CONF is set, the file it indicates will be used.)

        :param extra_conf_dict: (dict) dict for any extra spark conf settings,
                                for ex.  {"spark.hadoop.fs.default.name": "file:///"}
                                these will override any matching settings from extra_conf_file, if provided
        :param use_local_fs: (bool) simpler way to specify using local file system, rather than hdfs or other
        :param debug: (int or str) provide an port address to attach a debugger to the JVM that gets started
        :return: TkContext

        Creating a TkContext requires creating or obtaining a SparkContext object.  It is usually recommended to have
        the TkContext create the SparkContext, since it can provide the proper locations to the sparktk specific
        dependencies (i.e. jars).  Otherwise, specifying the classpath and jars arguments is left to the user.


        Examples
        --------

        <skip>
        Creating a TkContext using no arguments will cause a SparkContext to be created using default settings:

            >>> import sparktk

            >>> tc = sparktk.TkContext()

            >>> print tc.sc._conf.toDebugString()
            spark.app.name=sparktk
            spark.driver.extraClassPath=/opt/lib/spark/lib/*:/opt/spark-tk/sparktk-core/*
            spark.driver.extraLibraryPath=/opt/lib/hadoop/lib/native:/opt/lib/spark/lib:/opt/lib/hadoop/lib/native
            spark.jars=file:/opt/lib/spark/lib/spark-examples-1.6.0-hadoop2.6.0.jar,file:/opt/lib/spark/lib/spark-assembly.jar,file:/opt/lib/spark/lib/spark-examples.jar,file:/opt/lib/spark-tk/sparktk-core/sparktk-core-1.0-SNAPSHOT.jar,file:/opt/lib/spark-tk/sparktk-core/dependencies/spark-mllib_2.10-1.6.0.jar, ...
            spark.master=local[4]
            spark.yarn.jar=local:/opt/lib/spark/lib/spark-assembly.jar


        Another case with arguments to control some Spark Context settings:

            >>> import sparktk

            >>> tc = sparktk.TkContext(master='yarn-client',
            ...                        py_files='mylib.py',
            ...                        pyspark_submit_args='--jars /usr/lib/custom/extra.jar' \
            ...                                            '--driver-class-path /usr/lib/custom/*' \
            ...                                            '--executor-memory 6g',
            ...                        extra_conf={'spark.files.overwrite': 'true'},
            ...                        app_name='myapp'

            >>> print tc.sc._conf.toDebugString()
            spark.app.name=myapp
            spark.driver.extraClassPath=/usr/lib/custom/*:/opt/lib/spark/lib/*:/opt/spark-tk/sparktk-core/*
            spark.driver.extraLibraryPath=/opt/lib/hadoop/lib/native:/opt/lib/spark/lib:/opt/lib/hadoop/lib/native
            spark.executor.memory=6g
            spark.files.overwrite=true
            spark.jars=file:/usr/local/custom/extra.jar,file:/opt/lib/spark/lib/spark-examples-1.6.0-hadoop2.6.0.jar,file:/opt/lib/spark/lib/spark-assembly.jar,file:/opt/lib/spark/lib/spark-examples.jar,file:/opt/lib/spark-tk/sparktk-core/sparktk-core-1.0-SNAPSHOT.jar,file:/opt/lib/spark-tk/sparktk-core/dependencies/spark-mllib_2.10-1.6.0.jar, ...
            spark.master=yarn-client
            spark.yarn.isPython=true
            spark.yarn.jar=local:/opt/lib/spark/lib/spark-assembly.jar

        </skip>

        """
        if not sc:
            if SparkContext._active_spark_context:
                msg = "New context NOT created because there is already an active SparkContext"
                logger.warn(msg)
                import sys
                sys.stderr.write("[WARNING] %s\n" % msg)
                sys.stderr.flush()
                sc = SparkContext._active_spark_context
            else:
                sc = create_sc(master=master,
                               py_files=py_files,
                               spark_home=spark_home,
                               sparktk_home=sparktk_home,
                               pyspark_submit_args=pyspark_submit_args,
                               app_name=app_name,
                               other_libs=other_libs,
                               extra_conf_file=extra_conf_file,
                               extra_conf_dict=extra_conf_dict,
                               use_local_fs=use_local_fs,
                               debug=debug)
        if type(sc) is not SparkContext:
            if sc is TkContext.__mock:
                return
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
        #loggers.set_spark(self._sc, "off")  # todo: undo this/move to config, I just want it outta my face most of the time

    from sparktk.arguments import implicit

    @staticmethod
    def validate(tc, arg_name='tc'):
        """
        Validates that the given tc object is indeed a TkContext.  Raises a ValueError if it is not.

        Examples
        --------

            <hide>
            >>> from sparktk import TkContext

            </hide>

            >>> TkContext.validate(tc)

            >>> try:
            ...     TkContext(25)
            ... except TypeError:
            ...     print "Not a TkContext!"
            Not a TkContext!

        """
        # Since tc is so commonly used as an implicit variable, it's worth special code here to save a lot of imports
        require_type(TkContext, tc, arg_name)

    @staticmethod
    def _create_mock_tc():
        """
        Creates a TkContext which does NOT have a valid SparkContext

        (Useful for testing or exploring sparktk without having Spark around)
        """
        return TkContext(TkContext.__mock)

    @property
    def sc(self):
        """
        Access to the underlying SparkContext

        Example
        -------

            >>> tc.sc.version
            u'1.6.0'

        """
        return self._sc

    @property
    def sql_context(self):
        """
        Access to the underlying Spark SQLContext

        Example
        -------

        <skip>

            >>> tc.sql_context.registerDataFrameAsTable(frame.dataframe, "table1")
            >>> df2 = tc.sql_context.sql("SELECT field1 AS f1, field2 as f2 from table1")
            >>> df2.collect()
            [Row(f1=1, f2=u'row1'), Row(f1=2, f2=u'row2'), Row(f1=3, f2=u'row3')]

        </skip>

        """
        if self._sql_context is None:
            from pyspark.sql import SQLContext
            self._sql_context = SQLContext(self.sc)
        return self._sql_context

    @property
    def jutils(self):
        """Utilities for working with the remote JVM"""
        return self._jutils

    @property
    def agg(self):
        """
        Convenient access to the aggregation function enumeration (See the
        <a href="frame.m.html#sparktk.frame.frame.Frame.group_by">group_by operation</a> on sparktk Frames)

        Example
        -------

        For the given frame, count the groups in column 'b':

            <hide>
            >>> data = [[1, "alpha", 3.0],
            ...         [1, "bravo", 5.0],
            ...         [1, "alpha", 5.0],
            ...         [2, "bravo", 8.0],
            ...         [2, "charlie", 12.0],
            ...         [2, "bravo", 7.0],
            ...         [2, "bravo", 12.0]]
            >>> schema = [("a",int), ("b",str), ("c",float)]

            >>> frame = tc.frame.create(data, schema)

            </hide>

            >>> frame.inspect()
            [#]  a  b        c
            =====================
            [0]  1  alpha     3.0
            [1]  1  bravo     5.0
            [2]  1  alpha     5.0
            [3]  2  bravo     8.0
            [4]  2  charlie  12.0
            [5]  2  bravo     7.0
            [6]  2  bravo    12.0

            >>> b_count = frame.group_by('b', tc.agg.count)

            >>> b_count.inspect()
            [#]  b        count
            ===================
            [0]  alpha        2
            [1]  charlie      1
            [2]  bravo        4

        """
        from sparktk.frame.ops.group_by import agg
        return agg

    @property
    def frame(self):
        """
        Access to create or load the sparktk Frames  (See the <a href="frame.m.html">Frame API</a>)

        Example
        -------

            >>> frame = tc.frame.create([[1, 3.14, 'blue'], [7, 1.61, 'red'], [4, 2.72, 'yellow']])

            >>> frame.inspect()
            [#] C0  C1    C2
            =====================
            [0]  1  3.14  blue
            [1]  7  1.61  red
            [2]  4  2.72  yellow


            >>> frame2 = tc.frame.import_csv("../datasets/basic.csv")

            >>> frame2.inspect(5)
            [#]  C0   C1     C2  C3
            ================================
            [0]  132  75.4    0  correction
            [1]  133  77.66   0  fitness
            [2]  134  71.22   1  proposal
            [3]  201   72.3   1  utilization
            [4]  202   80.1   0  commission


        """
        return get_lazy_loader(self, "frame", implicit_kwargs={'tc': self}).frame  # .frame to account for extra 'frame' in name vis-a-vis scala

    @property
    def graph(self):
        """
        Access to create or load the sparktk Graphs (See the <a href="graph.m.html">Graph API</a>)

        Example
        -------

            <hide>
            >>> g = tc.examples.graphs.get_movie_graph()
            >>> g.save('sandbox/my_saved_graph')
            </hide>

            >>> g = tc.graph.load('sandbox/my_saved_graph')

        """
        return get_lazy_loader(self, "graph", implicit_kwargs={'tc': self}).graph  # .graph to account for extra 'graph' in name vis-a-vis scala

    @property
    def dicom(self):
        """
        Access to create or load the sparktk Dicom objects  (See the <a href="dicom.m.html">Dicom API</a>)

        Example
        -------

            <skip>
            >>> d = tc.dicom.import_dcm('path/to/dicom/images/')

            >>> type(d)
            sparktk.dicom.dicom.Dicom
            </skip>

        """
        return get_lazy_loader(self, "dicom", implicit_kwargs={'tc': self}).dicom  # .dicom to account for extra 'dicom' in name vis-a-vis scala


    @property
    def tensorflow(self):
        """
        """
        return get_lazy_loader(self, "tensorflow", implicit_kwargs={'tc': self}).tensorflow

    @property
    def models(self):
        """
        Access to create or load the various models available in sparktk  (See the <a href="models/index.html">Models API</a>)

        Examples
        --------

        Train an SVM model:

            <skip>
            >>> svm_model = tc.models.classification.svm.train(frame, 'label', ['data'])

        Train a Random Forest regression model:

            >>> rf = tc.models.regression.random_forest_regressor.train(frame,
            ...                                                         'Class',
            ...                                                         ['Dim_1', 'Dim_2'],
            ...                                                         num_trees=1,
            ...                                                         impurity="variance",
            ...                                                         max_depth=4,
            ...                                                         max_bins=100)

        Train a KMeans clustering model:

            >>> km = tc.models.clustering.kmeans.train(frame, ["data"], k=3)

            </skip>

        """
        return get_lazy_loader(self, "models", implicit_kwargs={'tc': self})

    @property
    def examples(self):
        """
        Access to some example data structures

        Example
        -------

        Get a small, built-in sparktk Frame object:

            >>> cities = tc.examples.frames.get_cities_frame()

            >>> cities.inspect(5)
            [#]  rank  city       population_2013  population_2010  change  county
            ==========================================================================
            [0]  1     Portland   609456           583776           4.40%   Multnomah
            [1]  2     Salem      160614           154637           3.87%   Marion
            [2]  3     Eugene     159190           156185           1.92%   Lane
            [3]  4     Gresham    109397           105594           3.60%   Multnomah
            [4]  5     Hillsboro  97368            91611            6.28%   Washington

        """
        return get_lazy_loader(self, "examples", implicit_kwargs={'tc': self})

    def load(self, path, validate_type=None):
        """
        Loads object from the given path

        Parameters
        ----------

        :param path: (str) location of the object to load
        :param validate_type: (type) if provided, a RuntimeError is raised if the loaded obj is not of that type
        :return: (object) the loaded object

        Example
        -------

        <skip>
            >>> f = tc.load("/home/user/sandbox/superframe")

            >>> type(f)
            sparktk.frame.frame.Frame

        </skip>

        """
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
        """
        Create a python object for the scala_obj

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

