"""Setup up tests for regression """
import unittest
import uuid
import datetime
import os

import sparktk as stk

import config
from threading import Lock

lock = Lock()
global_tc = None


def get_context():
    global global_tc
    with lock:
        if global_tc is None:
            global_tc = stk.TkContext()
    return global_tc


class SparkTKTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Build the context for use"""
        cls.context = get_context()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def get_file(self, filename):
        """Return the hdfs path to the given file"""
        # Note this is an HDFS path, not a userspace path. os.path library
        # may be wrong
        placed_path = "/user/" + config.user + "/qa_data/" + filename
        return placed_path

    def get_name(self, prefix):
        """build a guid hardened unique name """
        datestamp = datetime.datetime.now().strftime("%m_%d_%H_%M_")
        name = prefix + datestamp + uuid.uuid1().hex

        return name

    def get_local_dataset(self, dataset):
        """gets the dataset from the dataset folder"""
        this_directory = os.path.dirname(os.path.abspath(__file__))
        dataset_directory = os.path.dirname(os.path.dirname(this_directory)) + "/datasets/"
        return dataset_directory + dataset

    def assertFramesEqual(self, frame1, frame2):
        frame1_take = frame1.take(frame1.count())
        frame2_take = frame2.take(frame2.count())

        self.assertItemsEqual(frame1_take, frame2_take)
