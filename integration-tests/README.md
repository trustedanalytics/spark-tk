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


# Authoring Doc Examples for Test

 - Goal: keep doc examples up to date and working
 - Solution: execute doctest on examples as integration tests
 - What is doctest?  A python module which looks through text for what appears to be interactive python sessions and executes them.  Test results are based on comparing REPL output.  [https://docs.python.org/2/library/doctest.html](https://docs.python.org/2/library/doctest.html)

See `tests/doctgen.py` which is responsible for extracting the code snippets and forming testcases.

In general, any code snippets which use `>>> ` notation will be analyzed for testing.






### Example doctest input

    This is an example of testing within documentation

    >>> 1 + 1
    2

    Incorrectly two numbers and verify

    >>> 2 + 2
    5

    Multi-line input:

    >>> len([1, 2, 3, 4, 5,
    ...      6, 7, 8, 9, 10])
    10

    Indeterminate output:  (-etc- is keyword to ignore output)

    >>> import time
    >>> time.time()
    -etc-

    Forget a blankline after expected output

    >>> 3 + 3
    6
    This line of text comes too soon!


### Example doctest output

    Trying:
        1 + 1
    Expecting:
        2
    ok
    Trying:
        2 + 2
    Expecting:
        5
    **********************************************************************
    File "/dev/atk/integration-tests/tests/doc_api_examples_test.py"
    Failed example:
        2 + 2
    Expected:
        5
    Got:
        4
    Trying:
        len([1, 2, 3, 4, 5,
             6, 7, 8, 9, 10])
    Expecting:
        10
    ok
    Trying:
        import time
    Expecting nothing
    ok
    Trying:
        time.time()
    Expecting:
        -etc-
    ok
    Trying:
        3 + 3
    Expecting:
        6
        This line of text comes too soon!
    **********************************************************************
    File "/dev/atk/integration-tests/tests/doc_api_examples_test.py"
    Failed example:
        3 + 3
    Expected:
        6
        This line of text comes too soon!
    Got:
        6
    **********************************************************************
    1 items had failures:
       2 of   6 in doc_api_examples_test.py
    6 tests in 1 items.
    4 passed and 2 failed.
    ***Test Failed*** 2 failures.
    
    Failure
    Traceback (most recent call last):
      File "/dev/atk/integration-tests/tests/doc_api_examples_test.py", line 123, in test_doc_examples
        self.assertEqual(0, results.failed, "Tests in the example documentation failed.")
    AssertionError: Tests in the example documentation failed.
    
    
    Process finished with exit code 0

sparktk API documentation markup
--------------------------------

sparktk uses some custom markup tags in the documentation to control treatment for both testing and documentation publishing

. Need to skip testing some examples

  * Some examples will not practically run in an integration test env
  
  * doctest has +SKIP markup, but it is per-line only and we don't want its ugliness in the documentation
     `>>> frame.drop_columns('age')  # doctest: +SKIP`

. Need to hide test code (or any text) from the published documentation:
 
  * Some examples require setup code to run correctly which should not clutter the documentation
  
  
sparktk uses simple, single-line markup  (HTMLish).  A doc markup tag must take up an entire line and be preceded by
only whitespace.  The markup tags have different effects depending on the target: *doctest* or *doc*.  The *doctest*
target is when the examples are run for valdiation.  The *doc* target is when the examples are rendered for human
consumption, like HTML.

**`<hide>`** - hides text from the documentation:

     <hide>
     >>> import sparktk as tk
     >>> tc = tk.TkContext()
     </hide>
     
     >>> tc.load("myframe")

  becomes:

     >>> tc.load("myframe")

  in the documentation, yet the hidden lines execute during testing.


**`<skip>`** - skips code from executing in the tests

     Example: given a Hive table *person*, import its data into a Frame
                      
     <skip>
     >>> frame = tc.frame.import_hive("person")
     </skip>

   becomes:
   
     Example: given a Hive table *person*, import its data into a Frame
                      
     >>> frame = tc.frame.import_hive("person")

   in the documentation, and for testing it appears like:
   
     Example: given a Hive table *person*, import its data into a Frame
     
   which contains no code to execute
     
    

**`<progress>`** - alias for ellipsis marker `-etc-` which means the test will ignore output, however
provides a nicer progress indicator in the documentation

**`<blankline>`** - allows blanklines in the doctest results, i.e. the blankline will be validated and
not signal the end of the stdout comparison


## Example

For this example to test, we need to import sparktk and create a frame.  However, we don't want that to clutter the API documentation unnecessarily, so we surround it by `<hide></hide>`.  Notice the extra blank line after `-etc-` and before `</hide>` to close the doctest output eval.  Also, note the use of `<progress>` to mark when a message from an operation may occur.

    <hide>
    >>> import sparktk as tk
    >>> tc = tk.TkContext()
    -etc-
    
    >>> f = tc.frame.load([[1], [3], [1], [0], [2], [1], [4], [3]], [('numbers', ta.int32)]))
    -etc-
    
    </hide>
    Consider the following sample data set containing several numbers.
    
    >>> f.inspect()
    [#]  numbers
    ============
    [0]        1
    [1]        3
    [2]        1
    [3]        0
    [4]        2
    [5]        1
    [6]        4
    [7]        3
    
    >>> ecdf_frame = f.ecdf('numbers')
    <progress>
    
    >>> ecdf_frame.inspect()
    [#]  numbers  numbers_ECDF
    ==========================
    [0]        0         0.125
    [1]        1           0.5
    [2]        2         0.625
    [3]        3         0.875
    [4]        4           1.0
