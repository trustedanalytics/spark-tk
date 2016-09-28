
"""
Some very basic tests to see if things are generally working
"""

from setup import tc, rm, get_sandbox_path


def test_jconvert_option(tc):
    something = "Something"
    option = tc.jutils.convert.to_scala_option(something)
    back = tc.jutils.convert.from_scala_option(option)
    assert back == something


def test_smoke_take(tc):
    f = tc.frame.create([[1, "one"], [2, "two"], [3, "three"]])
    t = f.take(2)
    assert t == [[1, 'one'], [2, 'two']]
    #print "take=%s" % str(t)


def test_save_load(tc):
    path = get_sandbox_path("smoke_save_load")
    rm(path)
    frame1 = tc.frame.create([[2,"ab"],[1.0,"cd"],[7.4,"ef"],[1.0,"gh"],[9.0,"ij"],[2.0,"kl"],[0,"mn"],[6.0,"op"],[5.0,"qr"]],
                             [("data", float),("name", str)])
    frame1_inspect = frame1.inspect()
    frame1.save(path)
    frame2 = tc.load(path)
    frame2_inspect = frame2.inspect()
    assert(frame1_inspect, frame2_inspect)
    assert(str(frame1.schema), str(frame2.schema))


def test_back_and_forth_py_scala(tc):
    # python
    f = tc.frame.create([[1, "one"],
                         [2, "two"],
                         [3, "three"],
                         [4, "four"],
                         [5, "five"],
                         [6, "six"],
                         [7, "seven"],
                         [8, "eight"],
                         [9, "nine"],
                         [10, "ten"]],
                        [("a", int), ("b", str)])
    # python
    f.add_columns(lambda row: row.a + 4, ("c", int))
    # scala
    f.bin_column("a", [5, 8, 10.0, 30.0, 50, 80])
    # python
    f.filter(lambda row: row.a > 5)
    results = str(f.inspect())
    expected = """[#]  a   b      c   a_binned
============================
[0]   6  six    10         0
[1]   7  seven  11         0
[2]   8  eight  12         1
[3]   9  nine   13         1
[4]  10  ten    14         2"""
    assert(results == expected)


def test_row_count(tc):
    # create frame
    f = tc.frame.create([[item] for item in range(0, 10)],[("a", int)])
    # check row count (python)
    assert(f._is_python == True)
    assert(f.count() == 10)
    # to scala
    f._scala
    # check row count (scala)
    assert(f._is_python == False)
    assert(f.count() == 10)


def est_np(tc):
    # We can't use numpy numeric types and go successfully to Scala RDDs --the unpickler gets a constructor error:
    # Caused by: net.razorvine.pickle.PickleException: expected zero arguments for construction of ClassDict (for numpy.dtype)
    # todo: get this test working!
    # when it works, go back to dtypes and enable the np types
    import numpy as np
    f = tc.frame.create([[np.int32(1), "one"], [np.int32(2), "two"]], [("a", int), ("b", str)])  # schema intentionally int, not np.int32
    #print f.inspect()
    # force to_scala
    f.bin_column("a", [5, 8, 10.0, 30.0, 50, 80])
    # action
    #print f.inspect()  # chokes when bin_column triggers the switch to scala RDD

