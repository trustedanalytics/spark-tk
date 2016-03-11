from setup import tk_context

import os

root = "/home/blbarker/tmp"
victims = [os.path.join(root, f) for f in ["prdd", "jrdd", "raa", "rdd", "export", "binned", "transform", "scala_take"]]

def test_smoke_sparktk(tk_context):
    f = tk_context.to_frame([1,2, 3, 4])
    assert f.count() == 4


def test_smoke_take(tk_context):
    f = tk_context.to_frame([[1, "one"], [2, "two"], [3, "three"]])
    t = f.take(2)
    print "take=%s" % str(t)

def test_bin(tk_context):
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
    f.bin_column("a", [0.1, 0.1, 0.8])
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

