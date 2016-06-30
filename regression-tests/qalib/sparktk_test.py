"""Setup up tests for regression """
import unittest
import uuid
import datetime

import sparktk as stk

import config

# set up a singleton to share spark context across multiple test suites
# each py.test run can only run create context 1 time
global_spark_context = None
spark_context_initialized = False


def get_context():
    global global_spark_context
    global spark_context_initialized
    if not spark_context_initialized:
        global_spark_context = stk.TkContext()
        spark_context_initialized = True
    return global_spark_context


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
        placed_path = "/user/" + config.user + "/qa_data/" + filename
        return placed_path

    def get_name(self, prefix):
        """build a guid hardened unique name """
        datestamp = datetime.datetime.now().strftime("%m_%d_%H_%M_")
        name = prefix + datestamp + uuid.uuid1().hex + config.qa_suffix

        # ATK presently doesn't allow names over 127 characters in length
        if len(name) > 127:
            print "Warning: Name Length is over 127"

        return name

    def assertFramesEqual(self, frame1, frame2):
        frame1_take = frame1.take(frame1.row_count)
        frame2_take = frame2.take(frame2.row_count)

        self.assertItemsEqual(frame1_take, frame2_take)
