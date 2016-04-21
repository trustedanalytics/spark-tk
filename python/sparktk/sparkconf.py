import os
from pyspark import SparkContext, SparkConf

import logging
logger = logging.getLogger('sparktk')


def create_sc(master=None,
              jars='source_code',
              py_files='source_code',
              spark_home=None,
              pyspark_submit_args=None,
              app_name="sparktk"):
    """
    Creates a SparkContext with sparktk defaults

    Many parameters can be overwritten

    :param master: spark master setting
    :param jars: str of jar paths separated by a colon ':'  if jars == 'source_code' then jars will be taken from the
    spark-tk/core/target folder
    :param py_files: list of str of paths to python dependencies; if py_files == 'source_code' then the current python
    package will be freshly zipped up and put in the target folder for shipping by spark
    :param spark_home: override $SPARK_HOME
    :param app_name: name of spark app
    :return: pyspark SparkContext
    """

    def default_jars_if_source_code():
        """helper to create paths to the sparktk jars as if we're running in the source code dir structure"""

        d = os.path.dirname
        root = os.path.join(d(d(d(os.path.abspath(__file__)))))
        target = os.path.join(root, 'core/target')
        if os.path.isdir(target):
            logger.warn("create_sc() no jars specified, using jars as if running from source code using path %s", root)
            dirs = [target, os.path.join(target, 'dependencies')]  # add locations for jars here
            driver_class_path_str = ':'.join(["%s/*" % d for d in dirs])
            jar_files = [os.path.join(d, f) for d in dirs for f in os.listdir(d) if f.endswith('.jar')]
            jars_str = ','.join(jar_files)
            return jars_str, driver_class_path_str
        else:
            logger.warn("create_sc() could not find jars in %s.  You may need to specify appropriate jar paths", target)
        return None, None

    def set_env(name, value):
        """helper to set env w/ log"""
        logger.info("sparktk.create_sc() $%s=%s" % (name, value))
        os.environ[name] = value

    if not master:
        master = 'local[2]'
        logger.info("sparktk.create_sc() master not specified, setting to %s", master)

    if not spark_home:
        spark_home = os.environ.get('SPARK_HOME', '/opt/cloudera/parcels/CDH/lib/spark')
    set_env('SPARK_HOME', spark_home)

    if not pyspark_submit_args:
        pyspark_submit_args = os.environ.get('PYSPARK_SUBMIT_ARGS', '')

    if jars == 'source_code':
        jars, driver_class_path = default_jars_if_source_code()
    else:
        driver_class_path = None

    if jars and '--jars' not in pyspark_submit_args:
        if not driver_class_path:
            driver_class_path = jars.replace(',', ':')
        # Pyspark bug where --jars doesn't add to driver path  https://github.com/apache/spark/pull/11687
        # so we must create driver-class-path explicitly, fix targeted for Spark 2.0, back-port to 1.6 unlikely
        pyspark_submit_args += "--jars %s --driver-class-path %s pyspark-shell" % (jars, driver_class_path)

    if not pyspark_submit_args:
        pyspark_submit_args = "pyspark-shell"  # behavior of PYSPARK_SUBMIT_ARGS, needs this on the end

    set_env('PYSPARK_SUBMIT_ARGS', pyspark_submit_args)

    if not os.environ.get('PYSPARK_DRIVER_PYTHON'):
        set_env('PYSPARK_DRIVER_PYTHON', 'python2.7')

    if not os.environ.get('PYSPARK_PYTHON'):
        set_env('PYSPARK_PYTHON', 'python2.7')

    if py_files == 'source_code':
        from zip import zip_sparktk
        path = zip_sparktk()
        py_files = [path]
    if not py_files:
        py_files = []
    logger.info("sparktk.create_sc() py_files = %s", py_files)

    conf = SparkConf().setMaster(master).setAppName(app_name)

    # todo - add to logging
    print "=" * 80
    print "Creating SparkContext with the following SparkConf"
    print "pyFiles=%s" % str(py_files)
    print conf.toDebugString()
    print "=" * 80

    sc = SparkContext(conf=conf, pyFiles=py_files)
    return sc
