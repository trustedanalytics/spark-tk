from setup import tk_context, rm, get_sandbox_path
from sparktk.models.kmeans import KMeansModel

def est_smoke_take(tk_context):
    f = tk_context.to_frame([[1, "one"], [2, "two"], [3, "three"]])
    t = f.take(2)
    print "take=%s" % str(t)


def est_bin(tk_context):
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
    f.bin_column("a", [5, 8, 10.0, 30.0, 50, 80]) #, bin_column_name="super_fred")


    print f.inspect()


def est_kmeans(tk_context):
    frame = tk_context.to_frame([[2,"ab"],[1,"cd"],[7,"ef"],[1,"gh"],[9,"ij"],[2,"kl"],[0,"mn"],[6,"op"],[5,"qr"]],
                                [("data", float),("name", str)])
    print frame.inspect()
    model = KMeansModel(tk_context, frame, ["data"], [1], 3)
    print "==============================================================="
    print str(model)
    model.predict(frame)
    print "==============================================================="
    print frame.inspect()
    sizes = model.compute_sizes(frame)
    print "==============================================================="
    print "sizes=%s" % sizes
    print "==============================================================="
    print str(model)


def est_save_load(tk_context):
    path = get_sandbox_path("briton1")
    rm(path)
    frame1 = tk_context.to_frame([[2,"ab"],[1.0,"cd"],[7.4,"ef"],[1.0,"gh"],[9.0,"ij"],[2.0,"kl"],[0,"mn"],[6.0,"op"],[5.0,"qr"]],
                                [("data", float),("name", str)])
    frame1_inspect = frame1.inspect()
    frame1.save(path)
    frame2 = tk_context.load_frame(path)
    frame2_inspect = frame2.inspect()
    assert(frame1_inspect, frame2_inspect)
    assert(str(frame1.schema), str(frame2.schema))
    #print frame2.inspect()

def test_add_columns(tk_context):
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
    f.add_columns(lambda row: row.a + 4, ("c", int))
    print f.inspect()
    f.bin_column("a", [5, 8, 10.0, 30.0, 50, 80])
    print f.inspect()
    f.filter(lambda row: row.a > 5)
    print f.inspect()



