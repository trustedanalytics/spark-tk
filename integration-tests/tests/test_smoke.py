from setup import tk_context
from sparktk.dtypes import float32
from sparktk.models.kmeans import KMeansModel

import os

root = "/home/blbarker/tmp"
victims = [os.path.join(root, f) for f in ["prdd", "jrdd", "raa", "rdd", "export", "binned", "transform", "scala_take"]]

def est_smoke_sparktk(tk_context):
    f = tk_context.to_frame([1,2, 3, 4])
    assert f.count() == 4


def est_smoke_take(tk_context):
    f = tk_context.to_frame([[1, "one"], [2, "two"], [3, "three"]])
    t = f.take(2)
    print "take=%s" % str(t)

def est_bin(tk_context):
    rm_files()
    f = tk_context.to_frame([[1, "one"],
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
    #print f.count()
    f.bin_column("a", [5, 8, 10.0, 30.0, 50, 80]) #, bin_column_name="briton")
    # print "jtypestr for f._frame=%s" % tk_context.jtypestr(f._frame)
    # print "jtypestr for f._frame.rdd=%s" % tk_context.jtypestr(f._frame.rdd())
    # print "jtypestr for f._frame.schema=%s" % tk_context.jtypestr(f._frame.schema())
    # tk_context.jtypestr(f._frame)
    # print "schema=%s" % f.schema
    # t = f.take(4)
    # print "take=%s" % str(t)
    # cat_hdfs(os.path.join(root, "binned"))
    # cat_hdfs(os.path.join(root, "transform"))
    # cat_hdfs(os.path.join(root, "scala_take"))
    print f.inspect()


def cat_hdfs(file_name):
    print "-" * 79
    try:
        for f in [os.path.join(file_name,part) for part in os.listdir(file_name) if part.startswith("part")]:
            print f
            with open(f, "r") as r:
                print r.read()
    except Exception as e:
        print "Error with file '%s': %s" % (file_name, e)


def rm_files(files=None):
    if files is None:
        files = victims
    import shutil
    for file_name in files:
        try:
            shutil.rmtree(file_name)
        except:
            pass


def serialization(sc):
    return sc._jvm.org.trustedanalytics.at.serial.PythonSerialization

def est_schema_serialization(tk_context):
    from sparktk.dtypes import dtypes
    s = serialization(tk_context.sc)
    x = map(lambda t: [t[0], dtypes.to_string(t[1])], [("a", int), ("b", str)])  # convert dtypes to strings
    scala_schema = s.frameSchemaToScala(x)
    python_schema = s.frameSchemaToPython(scala_schema)
    print "python_schema=%s" % python_schema
    y = [(name, dtypes.get_from_string(dtype)) for name, dtype in python_schema]
    print "y=%s" % y

def est_serial(tk_context):
    export_file = "export"

    #files = [os.path.join(root, f) for f in ["prdd", "raa"]]
    #rm_files(files)
    #prdd = tk_context.sc.parallelize([[1, "one"], [2, "two"], [3, "three"], [4, "four"]])
    #prdd.saveAsTextFile(os.path.join(root, "prdd"))
    #raa = serialization(tk_context.sc).pythonToJava(prdd._jrdd)
    #pj = serialization(tk_context.sc).pj(prdd._jrdd)
    #pj.saveAsTextFile(os.path.join(root, "raa"))
    files = [os.path.join(root, f) for f in ["prdd", "jrdd", "raa", "rdd", export_file]]
    rm_files(files)
    f = tk_context.to_frame([[1, "one"], [2, "two"], [3, "three"], [4, "four"]], [("a", int), ("b", str)])
    f._frame.rdd.saveAsTextFile(os.path.join(root, "prdd"))
    f.export_to_csv(os.path.join(root, export_file))

    for file_name in files:
        cat_hdfs(file_name)

def test_kmeans(tk_context):
    frame = tk_context.to_frame([[2,"ab"],[1,"cd"],[7,"ef"],[1,"gh"],[9,"ij"],[2,"kl"],[0,"mn"],[6,"op"],[5,"qr"]],
                                [("data", float),("name", str)])
    print frame.inspect()
    model = KMeansModel(tk_context, frame, ["data"], [1], 3)
    print "KMeansModel...\n%s" % model

    model.predict(frame)
    print "==============================================================="
    print frame.inspect()


"""G
[8]   5.0  qr
           >>> model = ta.KMeansModel()
                       <progress>
>>> train_output = model.train(frame, ["data"], [1], 3)
                   <progress>
                   <skip>
>>> train_output
{u'within_set_sum_of_squared_error': 5.3, u'cluster_size': {u'Cluster:1': 5, u'Cluster:3': 2, u'Cluster:2': 2}}
</skip>
>>> train_output.has_key('within_set_sum_of_squared_error')
True
>>> predicted_frame = model.predict(frame, ["data"])
                      <progress>
>>> predicted_frame.column_names
    [u'data', u'name', u'distance_from_cluster_1', u'distance_from_cluster_2', u'distance_from_cluster_3', u'predicted_cluster']
    >>> model.publish()
    <progress>
"""
