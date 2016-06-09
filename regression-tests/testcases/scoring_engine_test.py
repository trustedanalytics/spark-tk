##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
"""
    Usage:  python2.7 scoring_engine_test.py
    Test scoring service over the network.
"""
# Validate basic operation of scoring models.

__author__ = 'Prune Wickart'
__credits__ = ["Prune Wickart"]
__version__ = "2015.09.29"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import requests


class ScoringServiceTest(unittest.TestCase):
    """Test fixture for the scoring service tests"""

    def setUp(self):
        super(ScoringServiceTest, self).setUp()

        self.engine_uri = 'http://10.54.8.NODE:PORT/v1/score'
        self.headers = {'Content-type': 'application/json',
                        'Accept': 'application/json,text/plain'}

    def tearDown(self):
        pass

    @unittest.skip("investigating error")
    def test_scoring_kmeans(self):
        """ Check scoring validity for KMeans model """
        # This is a KMeans model with 50 features and 6 classes,
        #   launched on port 9101 on kimyale's node01.

        # Remember that model class labels are slippery:
        #   the returned classes do not correspond to the training.

        model_port = "9110"
        model_node = "187"
        scoring_uri = self.engine_uri.replace("PORT", model_port)
        scoring_uri = scoring_uri.replace("NODE", model_node)
        # print scoring_uri

        # These test vectors are from the 6 classes of the training set,
        #   values rounded to the nearest integer.
        # They should generate one result from each of the 6 model classes.
        test_list = [
            "78,-93,-39,-11,-31,-50,49,12,41,99,-20,84,66,77,-59,32,60,"
            "3,15,39,49,31,-59,44,97,-15,3,-82,-19,-95,-13,-71,45,-15,"
            "33,90,-78,36,-2,5,18,-19,90,67,"
            "-5,44,34,-30,-70,47",           # class 0
            "13,13,84,12,-81,57,93,-12,53,71,61,-56,18,73,-36,79,32,-34,"
            "-11,-26,-41,-99,-83,20,-29,89,-4,47,-11,32,-13,14,-62,"
            "-29,61,13,88,93,-70,88,58,97,-24,"
            "6,-30,3,-49,-38,76,-69",       # class 1
            "-15,21,-85,-19,5,-82,69,5,-9,-78,-41,-43,84,-84,-96,-22,-4,"
            "-70,33,13,-26,67,23,-42,80,-28,83,-91,-93,-1,-15,-26,-67,"
            "3,75,-44,-17,-37,35,-52,-13,79,-41,"
            "-58,-12,-23,60,-18,44,-0",     # class 2
            "71,21,-72,-27,-72,-60,-33,-53,15,69,42,-38,86,-50,54,-27,-64,"
            "-53,-91,-29,-61,2,28,-3,-27,92,-95,-59,-53,99,23,85,-75,-59,"
            "34,29,-3,-99,40,-45,55,47,58,12,"
            "23,-11,-36,73,87,-70",         # class 3
            "-47,-72,-7,-41,8,0,8,-81,-87,-4,-84,-83,7,59,-29,63,-0,"
            "87,-41,-81,14,-69,22,23,20,20,-45,-76,98,94,57,-41,-30,-17,"
            "-54,-53,93,-37,-97,-99,46,81,81,"
            "81,-40,-98,15,-30,-28,-36",    # class 4
            "-7,-68,59,-20,-11,60,84,-73,-15,16,-37,-93,60,41,68,-17,-2,-80,"
            "35,35,-49,41,-79,82,13,85,60,55,1,-43,66,63,65,-95,54,"
            "-33,-46,97,-12,-65,-20,-1,-40,66,"
            "88,86,-78,1,-39,-78"           # class 5
        ]

        # Send each of the six vectors to the model.
        # Validate that we get one from each class, in any order.
        good_result = []
        for test_vec in test_list:
            scoring_vector = scoring_uri + '?data=' + test_vec
            print scoring_vector
            actual_result = requests.post(
                scoring_vector, headers=self.headers).text
            print "\tyields", actual_result
            good_result.append(int(actual_result[-2]))

        print good_result
        for k_class in range(1, len(test_list)+1):
            self.assertIn(k_class, good_result)

        # Blast all six vectors at once:
        scoring_vector = scoring_uri + '?data='
        for test_vec in test_list:
            scoring_vector += test_vec + "&data="
        scoring_vector = scoring_vector[:-6]
        # print scoring_vector
        result_all = requests.post(scoring_vector, headers=self.headers).text
        # This will look something like u'List(6, 5, 1, 4, 2, 3)'
        print result_all[4:]

        # Extract the six values.
        # Validate that we get one from each class, in any order.
        good_result = [int(_) for _ in result_all[5:-1].split(',')]
        for k_class in range(1, len(test_list)+1):
            self.assertIn(k_class, good_result)

    @unittest.skip("investigating time-out")
    def test_scoring_libsvm(self):
        """ Check scoring validity """
        # The shuttle-data model, an LibSVM model,
        #   is launched on port 9103 on kimyale's master node.
        # This is a 2-class model with 9 features.

        # Data within the training returns 1;
        # Other data returns -1.

        # Good data comes from the training set.
        good_data = '-0.777778, -0.0248585, -0.140625, 0.0140301,' \
                    '-0.275641, 0.025679, 0.124183, 0.252006, 0.144695'
        zero_data = '0, 0, 0, 0, 0, 0, 0, 0, 0'

        model_port = "9103"
        model_node = "188"
        scoring_uri = self.engine_uri.replace("PORT", model_port)
        scoring_uri = scoring_uri.replace("NODE", model_node)

        # Good data
        scoring_vector = \
            scoring_uri + '?data=' + good_data
        good_result = requests.post(scoring_vector, headers=self.headers).text
        print good_result
        self.assertEqual(u'List(1.0)', good_result)

        # Anomalous data
        scoring_vector = \
            scoring_uri + '?data=' + zero_data
        bad_result = requests.post(scoring_vector, headers=self.headers).text
        print bad_result
        self.assertEqual(u'List(-1.0)', bad_result)

        # Multiple requests
        for _ in range(5):
            scoring_vector += "&data=" + good_data + "&data=" + zero_data
        print scoring_vector
        big_result = requests.post(scoring_vector, headers=self.headers).text
        big_expect = u'List(-1.0, 1.0, -1.0, 1.0,' \
            ' -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0)'
        print big_result
        self.assertEqual(big_expect, big_result)

    @unittest.skip("missing test file; other problems may be masked.")
    def test_scoring_shuttle_stream(self):
        """ Check scoring validity """
        from collections import Counter

        # Validate the score distribution of the 7-class shuttle model.
        # We're worried about repeatability, not accuracy;
        #   for instance, the 100% failure on class 5 is not a concern here.

        model_port = "9103"
        model_node = "188"
        scoring_uri = self.engine_uri.replace("PORT", model_port)
        scoring_uri = scoring_uri.replace("NODE", model_node)

        # Good data comes from the training set.

        # Generate successive input lines
        flight_log = open("shuttle_scale_cut_true.csv", 'r')
        # flight_log = open("shuttle_scale_cut_tr", 'r')

        # Turn each line into a tuple of expected and actual scores
        # Expected score is in the first column, chars 0:3, range 1-7;
        # The data vector is the remainder of the line, chars 4:end.
        # Return value is of the form "list(n)", where n is the scored class.
        flight_score = ((int(float(line[:3])),
                        int(requests.post(scoring_uri + '?data=' + line[4:-1],
                                          headers=self.headers).text[5:-3]))
                        for line in flight_log)

        # Compare the scores and tally the results
        score_grade = ((expected, 1 if got == 1 else 0)
                       for expected, got in flight_score)
        score_count = Counter(score_grade)

        for row in range(8):
            print row, [score_count[(row, 0)], score_count[(row, 1)]]

        expected_count = {
            # (0, 0): 0, (0, 1): 0,
            (1, 0): 17, (1, 1): 23882,
            (2, 0): 2, (2, 1): 5,
            (3, 0): 10, (3, 1): 22,
            (4, 0): 7, (4, 1): 4703,
            (5, 0): 484,    # (5, 1): 0,
            (6, 0): 1,      # (6, 1): 0,
            (7, 0): 5, (7, 1): 5
        }

        self.assertEqual(expected_count, score_count)

if __name__ == '__main__':
    unittest.main()
