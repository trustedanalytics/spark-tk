from setup import tc, rm, get_sandbox_path
from sparktk.dtypes import float32
import os.path
import shutil
import subprocess
import json

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
    if(os.path.exists("jsonfile12") == True):
        shutil.rmtree("jsonfile12")
    frame.export_to_json("jsonfile12")
    assert(os.path.exists("jsonfile12") == False, "export_to_json should export frame in json format to jsonfile12 folder")
    print "Removing created file"
    shutil.rmtree("jsonfile12")



def test_strange_strings(tc):
    print "create frame"
    rows = [["A", "Hi, i'm here"],["B", "Hello's to everyone"],["C", "I'm good ~how are doing@"],["DD", "#$this is something, amazing''s"]]
    schema = [("name", str), ("message", str)]
    frame = tc.to_frame(rows, schema)

    assert(frame.row_count, 4, "frame should have 4 rows")
    assert(frame.column_names, ['name', 'message'])

    print "export frame in json format to local file system"
    if(os.path.exists("jsonstrangestring") == True):
        shutil.rmtree("jsonstrangestring")
    frame.export_to_json("jsonstrangestring")
    assert(os.path.exists("jsonstrangestring") == False, "export_to_json should export frame in json format to jsonstrangestring folder")

    data = subprocess.Popen("cat jsonstrangestring/* | grep 'DD'", stdout=subprocess.PIPE, shell=True).communicate()[0]
    json_data = json.loads(data)
    assert(json_data['message'] == frame.take(4).data[3][1], "the value for DD should be #$this is something, amazing''s")
    print "Removing created file"
    shutil.rmtree("jsonstrangestring")