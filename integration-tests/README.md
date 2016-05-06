# integration-tests

Tests that run at build time which exercise the API against a live Spark Context for basic coverage.  These tests
should execute quickly and as would be expected for a build machine to handle.  See *regression-tests* for more
sophisticated and scaled coverage.

To run the tests manually:

1. Make sure $SPARK_HOME is appropriately set or that the default is OK
2. `$ ./runtests.sh`


Options:

```
./runtests.sh -s                                                     # suppresses io capture, make log visible
./runtests.sh -k test_kmeans                                         # run individual test
./runtests.sh -k test_docs_python_sparktk_frame_ops_drop_columns_py  # run individual doc test
```
(See [pytest flags][1] for more options)

[1]:https://pytest.org/latest/usage.html

Note: If integration tests fail due to something like:
```
ValueError: Cannot run multiple SparkContexts at once; existing SparkContext(app=pytest-pyspark-local-testing,
master=local[2]) created by __init__ at /home/blbarker/dev/spark-tk/python/sparktk/sparkconf.py:102
```
Then this is most like due to a problem in the python code.  Try manually starting up a `TkContext` in a python
REPL session and debug from there.  i.e. It is most likely not a multiple SparkContexts problem.