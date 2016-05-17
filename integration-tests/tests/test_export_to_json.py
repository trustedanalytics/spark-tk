from setup import tc, rm, get_sandbox_path
from sparktk.dtypes import float32
import os.path

def test_export_to_json_file_path(tc):
    print "create frame"
    rows = [[1, 0.2, -2, 5], [2, 0.4, -1, 6], [3, 0.6, 0, 7], [4, 0.8, 1, 8]]
    schema = [('a', int), ('b', float),('c', int) ,('d', int)]
    frame = tc.to_frame(rows, schema)

    assert(frame.row_count, 4, "frame should have 4 rows")
    assert(frame.column_names, ['a', 'b', 'c', 'd'])

    print "compute dot product"
    frame.dot_product(['a','b'],['c','d'],'dot_product')
    assert(frame.column_names[4], "dot_product", "frame must have last column name as dot_product")

    print "export dp frame in json format to local file system"
    frame.export_to_json("jsonfile12")
    assert(os.path.exists("jsonfile12") == False, "export_to_json should export frame in json format to jsonfile12 folder")