from setup import tc, rm, get_sandbox_path
from sparktk.dtypes import float32
import os.path
import shutil
import subprocess
import json
import logging
logger = logging.getLogger(__name__)


def test_export_to_json_file_path(tc):
    logger.info("create frame")
    rows = [[1, 0.2, -2, 5], [2, 0.4, -1, 6], [3, 0.6, 0, 7], [4, 0.8, 1, 8]]
    schema = [('a', int), ('b', float),('c', int) ,('d', int)]
    frame = tc.frame.create(rows, schema)

    assert(frame.row_count, 4, "frame should have 4 rows")
    assert(frame.column_names, ['a', 'b', 'c', 'd'])

    logger.info("compute dot product")
    frame.dot_product(['a','b'],['c','d'],'dot_product')
    assert(frame.column_names[4], "dot_product", "frame must have last column name as dot_product")

    logger.info("export dp frame in json format to local file system")
    if(os.path.exists("jsonfile12") == True):
        shutil.rmtree("jsonfile12")
    frame.export_to_json("jsonfile12")
    assert(os.path.exists("jsonfile12") == False, "export_to_json should export frame in json format to jsonfile12 folder")
    logger.info("Removing created file")
    shutil.rmtree("jsonfile12")



def test_strange_strings(tc):
    logger.info("create frame")
    rows = [["A", "Hi, i'm here"],["B", "Hello's to everyone"],["C", "I'm good ~how are doing@"],["DD", "#$this is something, amazing''s"],['EE', 'He said, "Hello!"'],["FF", "u'It is 15 \u00f8c outside'"]]
    schema = [("name", str), ("message", str)]
    frame = tc.frame.create(rows, schema)

    assert(frame.row_count, 6, "frame should have 6 rows")
    assert(frame.column_names, ['name', 'message'])
    dir_name = "sandbox/json_strange_string"
    logger.info("export frame in json format to local file system")
    if(os.path.exists(dir_name) == True):
        shutil.rmtree(dir_name)
    frame.export_to_json(dir_name)
    assert(os.path.exists(dir_name) == False, "export_to_json should export frame in json format to %s folder" % dir_name)

    data = subprocess.Popen("cat %s/* | grep 'DD'" % dir_name, stdout=subprocess.PIPE, shell=True).communicate()[0]
    json_data = json.loads(str(data))
    assert(json_data['message'] == frame.take(6).data[3][1], "the value for DD should be #$this is something, amazing''s")

    data1 = subprocess.Popen("cat %s/* | grep 'EE'" % dir_name, stdout=subprocess.PIPE, shell=True).communicate()[0]
    json_data1 = json.loads(str(data1))
    assert(json_data1['message'] == frame.take(6).data[4][1], "the value for EE should be He said, \\\"Hello!\\\"")

    data2 = subprocess.Popen("cat %s/* | grep 'FF'" % dir_name, stdout=subprocess.PIPE, shell=True).communicate()[0]
    json_data2 = json.loads(str(data2))
    assert(json_data2['message'] == frame.take(6).data[5][1], "the value for FF should be u'It is 15 \u00f8c outside'")

    logger.info("Removing created file")
    shutil.rmtree(dir_name)