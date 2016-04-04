from setup import tc, rm, get_sandbox_path
from sparktk.models.kmeans import KMeans
from sparktk.dtypes import int32, float32

def test_smoke_take(tc):
    f = tc.to_frame([[1, "one"], [2, "two"], [3, "three"]])
    t = f.take(2)
    print "take=%s" % str(t)

def test_jconvert_option(tc):
    something = "Something"
    option = tc.jutils.convert.to_scala_option(something)
    back = tc.jutils.convert.from_scala_option(option)
    assert back == something

def test_bin(tc):
    f = tc.to_frame([[1, "one"],
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
    f.bin_column("a", [5, 8, 10.0, 30.0, 50, 80]) #, bin_column_name="super_fred")
    print f.inspect()


def test_kmeans(tc):

    frame = tc.to_frame([[2, "ab"],
                         [1,"cd"],
                         [7,"ef"],
                         [1,"gh"],
                         [9,"ij"],
                         [2,"kl"],
                         [0,"mn"],
                         [6,"op"],
                         [5,"qr"]],
                        [("data", float), ("name", str)])
    model = KMeans.train(tc, frame, ["data"], 3, seed=5)
    assert (model.k == 3)

    sizes = model.compute_sizes(frame)
    assert (sizes == [4, 1, 4])

    wsse = model.compute_wsse(frame)
    assert (wsse == 9.75)

    model.predict(frame)
    frame_inspect = str(frame.inspect())
    assert (frame_inspect == """[#]  data  name  cluster
========================
[0]   2.0  ab          0
[1]   1.0  cd          0
[2]   7.0  ef          1
[3]   1.0  gh          0
[4]   9.0  ij          1
[5]   2.0  kl          0
[6]   0.0  mn          2
[7]   6.0  op          1
[8]   5.0  qr          1""")

    model.add_distance_columns(frame)
    print frame.inspect()
    frame_inspect = str(frame.inspect())
    assert (frame_inspect == """[#]  data  name  cluster  distance0  distance1  distance2
=========================================================
[0]   2.0  ab          0       0.25    22.5625        4.0
[1]   1.0  cd          0       0.25    33.0625        1.0
[2]   7.0  ef          1      30.25     0.0625       49.0
[3]   1.0  gh          0       0.25    33.0625        1.0
[4]   9.0  ij          1      56.25     5.0625       81.0
[5]   2.0  kl          0       0.25    22.5625        4.0
[6]   0.0  mn          2       2.25    45.5625        0.0
[7]   6.0  op          1      20.25     0.5625       36.0
[8]   5.0  qr          1      12.25     3.0625       25.0""")


def test_save_load(tc):
    path = get_sandbox_path("briton1")
    rm(path)
    frame1 = tc.to_frame([[2,"ab"],[1.0,"cd"],[7.4,"ef"],[1.0,"gh"],[9.0,"ij"],[2.0,"kl"],[0,"mn"],[6.0,"op"],[5.0,"qr"]],
                         [("data", float),("name", str)])
    frame1_inspect = frame1.inspect()
    frame1.save(path)
    frame2 = tc.load_frame(path)
    frame2_inspect = frame2.inspect()
    assert(frame1_inspect, frame2_inspect)
    assert(str(frame1.schema), str(frame2.schema))
    #print frame2.inspect()


def est_np(tc):
    # We can't use numpy numeric types and go successfully to Scala RDDs --the unpickler gets a constructor error:
    # Caused by: net.razorvine.pickle.PickleException: expected zero arguments for construction of ClassDict (for numpy.dtype)
    # todo: get this test working!
    # when it works, go back to dtypes and enable the np types
    import numpy as np
    f = tc.to_frame([[np.int32(1), "one"], [np.int32(2), "two"]], [("a", int), ("b", str)])  # schema intentionally int, not np.int32
    print f.inspect()
    # force to_scala
    f.bin_column("a", [5, 8, 10.0, 30.0, 50, 80])
    # action
    print f.inspect()  # chokes when bin_column triggers the switch to scala RDD


def test_back_and_forth_py_scala(tc):
    # python
    f = tc.to_frame([[1, "one"],
                     [2, "two"],
                     [3, "three"],
                     [4, "four"],
                     [5, "five"],
                     [6, "six"],
                     [7, "seven"],
                     [8, "eight"],
                     [9, "nine"],
                     [10, "ten"]],
                     [("a", int32), ("b", str)])
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



