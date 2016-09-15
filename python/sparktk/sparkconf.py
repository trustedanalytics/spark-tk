"""Sets up Spark Context"""

import os
import shutil
import atexit
import glob2
from pyspark import SparkContext, SparkConf
from zip import zip_sparktk

LIB_DIR="dependencies"
SPARK_ASSEMBLY_SEARCH="**/spark-assembly*.jar"
CORE_TARGET="sparktk-core/target"
import logging
logger = logging.getLogger('sparktk')

def get_source_code_target_dir():
    """gets the core/target folder as if this is running from source code"""
    d = os.path.dirname
    root = os.path.join(d(d(d(os.path.abspath(__file__)))))
    target = os.path.join(root, CORE_TARGET)
    return target


# default values -- DO NOT CHANGE freely, instead change the environ variables
default_spark_home = '/opt/cloudera/parcels/CDH/lib/spark'
default_sparktk_home = get_source_code_target_dir()
default_spark_master = 'local[4]'


def set_env(name, value):
    """helper to set env w/ log"""
    logger.info("sparktk.sparkconf making $%s=%s" % (name, value))
    os.environ[name] = value


def get_jars_and_classpaths(dirs):
    """
    Helper which creates a tuple of two strings for the given dirs:

    1. jars string - a comma-separated list of all the .jar files in the given directories
    2. classpath string - a colon-separate list of all the given directories with a /* wildcard added

    :param dirs: a str or list of str specifying the directors to use for building the jar strings
    :return: (jars, classpath)
    """
    classpath = ':'.join(["%s/*" % d for d in dirs])
    jar_files = [os.path.join(d, f) for d in dirs for f in os.listdir(d) if f.endswith('.jar')]
    jars = ','.join(jar_files)
    return jars, classpath

def get_spark_dirs():
    try:
        spark_home = os.environ['SPARK_HOME']
    except KeyError:
        raise RuntimeError("Missing value for environment variable SPARK_HOME.")

    spark_assembly_search = glob2.glob(os.path.join(spark_home,SPARK_ASSEMBLY_SEARCH))
    if len(spark_assembly_search) > 0:
        spark_assembly = os.path.dirname(spark_assembly_search[0])
    else:
        raise RuntimeError("Couldn't find spark assembly jar")

    return [spark_assembly]


def get_sparktk_dirs():
    """returns the folders which contain all the jars required to run sparktk"""
    # todo: revisit when packaging is resolved, right now this assumes source code/build folder structure

    try:
        sparktk_home = os.environ['SPARKTK_HOME']
    except KeyError:
        raise RuntimeError("Missing value for SPARKTK_HOME.  Try setting $SPARKTK_HOME or the kwarg 'sparktk_home'")

    dirs = [sparktk_home,
            os.path.join(sparktk_home, LIB_DIR)]   # the /dependencies folder
    return dirs


def print_bash_cmds_for_sparktk_env():
    """prints export cmds for each env var set by set_env_for_sparktk, for use in a bash script"""
    # see ../gopyspark.sh
    for name in ['SPARK_HOME',
                 'SPARKTK_HOME',
                 'PYSPARK_PYTHON',
                 'PYSPARK_DRIVER_PYTHON',
                 'PYSPARK_SUBMIT_ARGS',
                 'SPARK_JAVA_OPTS',
                 ]:
        value = os.environ.get(name, None)
        if value:
            print "export %s='%s'" % (name, value)  # require the single-quotes because of spaces in the values


def set_env_for_sparktk(spark_home=None,
                        sparktk_home=None,
                        pyspark_submit_args=None,
                        other_libs=None,
                        debug=None):

    """Set env vars necessary to start up a Spark Context with sparktk"""

    if spark_home:
        set_env('SPARK_HOME', spark_home)
    elif 'SPARK_HOME' not in os.environ:
        set_env('SPARK_HOME', default_spark_home)

    if sparktk_home:
        set_env('SPARKTK_HOME', sparktk_home)
    elif 'SPARKTK_HOME' not in os.environ:
        set_env('SPARKTK_HOME', default_sparktk_home)

    if not os.environ.get('PYSPARK_DRIVER_PYTHON'):
        set_env('PYSPARK_DRIVER_PYTHON', 'python2.7')

    if not os.environ.get('PYSPARK_PYTHON'):
        set_env('PYSPARK_PYTHON', 'python2.7')

    # Validate other libraries to verify they have the required functions
    other_libs = _validate_other_libs(other_libs)

    # Everything else go in PYSPARK_SUBMIT_ARGS
    spark_dirs = get_spark_dirs()
    spark_dirs.extend(get_sparktk_dirs())

    # Get library directories from other_libs
    if other_libs is not None:
        for other_lib in other_libs:
            other_lib_dirs = other_lib.get_library_dirs()
            spark_dirs.extend(other_lib_dirs)

    jars, driver_class_path = get_jars_and_classpaths(spark_dirs)

    if not pyspark_submit_args:
        using_env = True
        pyspark_submit_args = os.environ.get('PYSPARK_SUBMIT_ARGS', '')
    else:
        using_env = False

    pieces = pyspark_submit_args.split()
    if ('--jars' in pieces) ^ ('--driver-class-path' in pieces):
        # Pyspark bug where --jars doesn't add to driver path  https://github.com/apache/spark/pull/11687
        # fix targeted for Spark 2.0, back-port to 1.6 unlikely
        msg = "If setting --jars or --driver-class-path in pyspark_submit_args, both must be set (due to Spark): "
        if using_env:
            msg += "$PYSPARK_SUBMIT_ARGS=%s" % os.environ['PYSPARK_SUBMIT_ARGS']
        else:
            msg += "pyspark_submit_args=%s" % pyspark_submit_args
        raise ValueError(msg)

    jars_value_index = next((i for i, x in enumerate(pieces) if x == '--jars'), -1) + 1
    if jars_value_index > 0:
        pieces[jars_value_index] = ','.join([pieces[jars_value_index], jars])
        driver_class_path_value_index = pieces.index('--driver-class-path') + 1
        pieces[driver_class_path_value_index] = ':'.join([pieces[driver_class_path_value_index], driver_class_path])
    else:
        pieces = ['--jars', jars, '--driver-class-path', driver_class_path]

    pyspark_submit_args = ' '.join(pieces)

    set_env('PYSPARK_SUBMIT_ARGS', pyspark_submit_args)

    if debug:
        print "Adding args for remote java debugger"
        try:
            address = int(debug)
        except:
            address = 5005  # default
        details = '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=%s' % address
        set_env('SPARK_JAVA_OPTS', details)


def create_sc(other_libs=None,
              master=None,
              py_files=None,
              spark_home=None,
              sparktk_home=None,
              pyspark_submit_args=None,
              app_name="sparktk",
              extra_conf=None,
              use_local_fs=False,
              debug=None):
    """
    Creates a SparkContext with sparktk defaults

    Many parameters can be overwritten

    :param other_libs: other libraries that will be used along with spark-tk, which need to be added to the class path
    :param master: spark master setting
    :param py_files: list of str of paths to python dependencies; Note the the current python
    package will be freshly zipped up and put in a tmp folder for shipping by spark, and then removed
    :param spark_home: override $SPARK_HOME
    :param sparktk_home: override $SPARKTK_HOME
    :param app_name: name of spark app
    :param extra_conf: dict for any extra spark conf settings, for ex. {"spark.hadoop.fs.default.name": "file:///"}
    :param use_local_fs: simpler way to specify using local file system, rather than hdfs or other
    :param debug: provide an port address to attach a debugger to the JVM that gets started
    :return: pyspark SparkContext
    """

    set_env_for_sparktk(spark_home, sparktk_home, pyspark_submit_args, other_libs, debug)

    # bug/behavior of PYSPARK_SUBMIT_ARGS requires 'pyspark-shell' on the end --check in future spark versions
    set_env('PYSPARK_SUBMIT_ARGS', ' '.join([os.environ['PYSPARK_SUBMIT_ARGS'], 'pyspark-shell']))

    if not master:
        master = default_spark_master
        logger.info("sparktk.create_sc() master not specified, setting to %s", master)

    conf = SparkConf().setMaster(master).setAppName(app_name)
    if extra_conf:
        for k, v in extra_conf.items():
            conf = conf.set(k, v)

    if use_local_fs:
        conf.set("spark.hadoop.fs.default.name", "file:///")

    if not py_files:
        py_files = []

    # zip up the relevant pieces of sparktk and put it in the py_files...
    path = zip_sparktk()
    tmp_dir = os.path.dirname(path)
    logger.info("sparkconf created tmp dir for sparktk.zip %s" % tmp_dir)
    atexit.register(shutil.rmtree, tmp_dir)  # make python delete this folder when it shuts down

    py_files.append(path)

    msg = '\n'.join(["=" * 80,
                     "Creating SparkContext with the following SparkConf",
                     "pyFiles=%s" % str(py_files),
                     conf.toDebugString(),
                     "=" * 80])
    logger.info(msg)

    sc = SparkContext(conf=conf, pyFiles=py_files)

    return sc

def _validate_other_libs(other_libs):
    """
    Validates the other_libs parameter.  Makes it a list, if it isn't already and verifies that all the items in the
    list are python modules with the required functions.

    Raises a TypeError, if the other_libs parameter is not valid.

    :param other_libs: parameter to validate
    :return: validated other_libs parameter
    """
    if other_libs is not None:
        if not isinstance(other_libs, list):
            other_libs = [other_libs]
        import types
        required_functions = ["get_loaders","get_main_object","get_library_dirs"]
        for lib in other_libs:
            if not isinstance(lib, types.ModuleType):
                raise TypeError("Expected other_libs to contain python modules, but received %s." % type(lib) )
            for required_function in required_functions:
                if not hasattr(lib, required_function):
                    raise TypeError("other_lib '%s' is missing %s() function." % (lib.__name__,required_function))
    return other_libs