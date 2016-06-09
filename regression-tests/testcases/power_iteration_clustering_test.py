##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015, 2016 Intel Corporation All Rights Reserved.
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
   Usage:  python2.7 power_iteration_clustering_test.py
   Test interface functionality of
        Frame.power_iteration_clustering()
"""

# Functionality tested:
#   Simple graphs
#     root node with triangle children
#     8 nodes in a line
#     9 equidistant points
#     binary tree (depth 5)
#     two isolated nodes
#     three disconnected squares
#     square matrix with diagonal alley
#   Classic clustering cases
#     k-means 4-cluster test
#     Mickey head
#     ball-and-arc
#     two arches
#     three rings
#     three circles
#   Special cases
#     null graph
#     1-node graph (edge to self)
#     1-edge graph (2 nodes)
#     disconnected triangles
#     isolated nodes
#     conflicting distances between nodes
#   Negative testing
#     negative, Nan, Inf distances
#     Non-positive iteration value
#     Bogus initialization mode
#     Bogus column name
#     k value <= 1
#     Null frame
#     Empty frame
#     Single-point frame
#     Single-edge frame -- legal
#     k > node count -- legal


__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2016.01.22"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import math

import unittest
import time

import trustedanalytics as ta

# from qalib import common_utils
from qalib import frame_utils
from qalib import atk_test


def string2dict(node_str):
    """turn a string list into a dictionary"""
    no_bracket = node_str[1:-1]
    assign_list = no_bracket.split(',')
    attrib_list = [i.split('=') for i in assign_list]
    attrib_dict = {str(key.strip()): str(value)
                   for (key, value) in attrib_list}
    return attrib_dict


def complete_graph(point_list):
    """Make a complete graph from a list of points"""
    """
    Input: list of tuples of point coordinates
    Return: edge list with 1/distance affinities
    """

    edge_list = []
    dim = len(point_list[0])    # space dimension
    for j in range(len(point_list)):
        for i in range(j):
            dist = math.sqrt(sum([(point_list[i][n] - point_list[j][n])**2 for n in range(dim)]))
            weight = math.atan(1/dist) if dist > 0 else math.pi/2
            edge_list.append([i, j, weight])

    return edge_list


class PowerIterationTest(atk_test.ATKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(PowerIterationTest, self).setUp()
        self.graph_schema = [("Source", ta.int64),
                             ("Destination", ta.int64),
                             ("Similarity", ta.float32)]

    def generate_binary_graph(self, depth):
        """
        Write a csv file of a binary tree's edge list, given the depth.
        :param depth: depth of binary tree
        :return: None
        """
        block_data = []
        for node_id in range(1, 2**depth):
            bin_rep = (bin(node_id)[2:])[::-1]
            sim_factor = 1.0 - bin_rep.index('1')*(1.0/depth)
            block_data.append([node_id-1, node_id, sim_factor])

        return block_data

    # def pp_edge_list(self, feature_list):
    #     """Pretty-print an edge list."""
    #     print "EDGE FEATURE LIST", feature_list.row_count, "items\n", \
    #         feature_list.inspect(feature_list.row_count)

    def build_edge_list(self, test_name, verbose=0, block_data=None):
        """Build the graph for the named test case."""

        trace = verbose > 0
        # report = verbose > 1

        frame = frame_utils.build_frame(
            block_data, self.graph_schema, self.prefix, file_format="list")
        if trace:
            print "Test", test_name
            print frame.inspect()

        return frame

    def run_normal_case(self, test_name, block_data=None, inspect_count=None, k=3, initialization_mode=u"random"):
        """Run a straightforward test case"""

        edge_list = self.build_edge_list(test_name, verbose=1, block_data=block_data)
        model = ta.PowerIterationClusteringModel()
        predict_result = model.predict(edge_list, u'Source', u'Destination', u'Similarity',
                                       k=k, initialization_mode=initialization_mode)
        class_frame = predict_result['predicted_frame']
        class_frame.sort('id')
        if inspect_count is None:
            inspect_count = class_frame.row_count
        print class_frame.inspect(inspect_count)
        print predict_result['number_of_clusters'], "clusters"
        print "Cluster sizes:", predict_result['cluster_size']

        return predict_result

    def test_doc_example(self):
        # Example from the API documentation

        block_data = [
            [1, 2, 1.0],
            [1, 3, 0.3],
            [2, 3, 0.3],
            [3, 0, 0.03],
            [0, 5, 0.01],
            [5, 4, 0.3],
            [5, 6, 1.0],
            [4, 6, 0.3]
        ]
        schema = [
            ('Source', ta.int64),
            ('Destination', ta.int64),
            ('Similarity', ta.float64)
        ]

        frame = frame_utils.build_frame(block_data, schema, file_format="list")
        frame.inspect()
        model = ta.PowerIterationClusteringModel()
        predict_result = model.predict(frame, u'Source', u'Destination', u'Similarity', k=3)
        print predict_result['predicted_frame'].inspect()
        census = predict_result['cluster_size']
        pop_spread = sorted(census.values())
        print "counts", pop_spread
        self.assertEquals([1, 2, 4], pop_spread)

    def test_line8(self):
        """Linear graph of 8 nodes"""
        run_label = 'line 8'
        block_data = [
            [1, 2, 0.01],
            [2, 3, 0.02],
            [3, 4, 0.03],
            [4, 5, 0.05],
            [5, 6, 0.08],
            [6, 7, 0.13],
            [7, 8, 0.21]
        ]
        predict_result = self.run_normal_case(run_label, block_data)
        class_frame = predict_result['predicted_frame']
        print class_frame.inspect(class_frame.row_count)
        census = predict_result['cluster_size']
        print "census", census
        pop_spread = sorted(census.values())
        print "counts", pop_spread
        # self.assertEquals([1, 2, 6], pop_spread)

    def test_equidistant(self):
        """Graph with all nodes equidistant"""
        run_label = 'equidistant 9'
        block_data = [
            [1, 2, 1], [1, 3, 1], [1, 4, 1], [1, 5, 1], [1, 6, 1], [1, 7, 1], [1, 8, 1], [1, 9, 1],
            [2, 3, 1], [2, 4, 1], [2, 5, 1], [2, 6, 1], [2, 7, 1], [2, 8, 1], [2, 9, 1],
            [3, 4, 1], [3, 5, 1], [3, 6, 1], [3, 7, 1], [3, 8, 1], [3, 9, 1],
            [4, 5, 1], [4, 6, 1], [4, 7, 1], [4, 8, 1], [4, 9, 1],
            [5, 6, 1], [5, 7, 1], [5, 8, 1], [5, 9, 1],
            [6, 7, 1], [6, 8, 1], [6, 9, 1],
            [7, 8, 1], [7, 9, 1],
            [8, 9, 1]
        ]

        # Validate initialization mode. too
        predict_result = self.run_normal_case(run_label, block_data, k=3,
                                              initialization_mode=u"degree")
        class_frame = predict_result['predicted_frame']
        print class_frame.inspect(class_frame.row_count)
        census = predict_result['cluster_size']
        pop_spread = sorted(census.values())
        self.assertEquals([9], pop_spread)

    def test_tree_depth5(self):
        """Binary tree graph of 2^N nodes"""
        run_label = 'tree 32'
        depth = 5
        data = self.generate_binary_graph(depth)
        start = time.time()
        predict_result = self.run_normal_case(run_label,
                                              block_data=data,
                                              k=depth+1)
        end = time.time()
        print "TIMING: case", run_label, "took", end-start

        # class_frame = predict_result['predicted_frame']
        # print class_frame.inspect(class_frame.row_count)
        census = predict_result['cluster_size']
        print "census", census
        pop_spread = sorted(census.values())
        print "counts", pop_spread
        self.assertEquals([2, 2, 2, 4, 8, 14], pop_spread)

    def test_isolated_node(self):
        """Solitons form their own cluster."""

        data = [
            [1, 2, 1.0],
            [3, 1, 0.3],
            [2, 3, 0.3],
            [0, 3, 0.1],
            [0, 6, 0.1],
            [4, 5, 1.0],
            [6, 4, 0.3],
            [5, 4, 0.3],
            [7, 5, 0],
            [8, 5, 0]
        ]

        # Nodes 7 and 8 are in their own cluster;
        # Node 0 is alone
        predict_result = self.run_normal_case('tri-tree', block_data=data)
        # class_frame = predict_result['predicted_frame']
        # print class_frame.inspect(class_frame.row_count)
        census = predict_result['cluster_size']
        pop_spread = sorted(census.values())
        self.assertEquals([1, 2, 6], pop_spread)

    def test_three_squares(self):
        point_list = [
            (0, 0),
            (0, 1),
            (1, 0),
            (1, 1),
            (3, 3),
            (3, 4),
            (4, 3),
            (4, 4),
            (8, 8),
            (8, 9),
            (9, 8),
            (9, 9)
        ]
        predict_result = self.run_normal_case('three squares', block_data=complete_graph(point_list))
        # class_frame = predict_result['predicted_frame']
        # print class_frame.inspect(class_frame.row_count)
        census = predict_result['cluster_size']
        pop_spread = sorted(census.values())
        print pop_spread
        # Middle element should be 4; ends may be (4, 4) or (3, 5)
        self.assertEquals(4, pop_spread[1])

    def test_rings(self):

        model = ta.PowerIterationClusteringModel()
        edge_frame = frame_utils.build_frame("cluster_three_rings.csv",
                                             self.graph_schema, skip_header_lines=1)
        predict_result = model.predict(edge_frame,
                                       u'Source', u'Destination', u'Similarity',
                                       k=3)

        census = predict_result['cluster_size']
        print "census", census
        pop_spread = sorted(census.values())
        print "counts", pop_spread
        self.assertEquals([4, 48, 80], pop_spread)

    def test_fist_and_cup(self):
        """Classic data set of round cluster on left,
            crescent on the right, and some outliers."""

        model = ta.PowerIterationClusteringModel()
        edge_frame = frame_utils.build_frame("cluster_fist_cup.csv",
                                             self.graph_schema, skip_header_lines=1)

        predict_result = model.predict(edge_frame,
                                       u'Source', u'Destination', u'Similarity',
                                       k=2)
        census = predict_result['cluster_size']
        print "census", census
        pop_spread = sorted(census.values())
        print "counts", pop_spread
        # Ensure that each natural cluster contains at least its own elements,
        #   as well as some outliers
        self.assertLess(100, pop_spread[0])
        self.assertLess(300, pop_spread[1])

    def test_three_clusters(self):
        """Classic data set of large cluster, small satellite, and
            remote satellite."""

        model = ta.PowerIterationClusteringModel()
        edge_frame = frame_utils.build_frame("cluster_three_blobs.csv",
                                             self.graph_schema, skip_header_lines=1)
        predict_result = model.predict(edge_frame,
                                       u'Source', u'Destination', u'Similarity',
                                       k=3)

        census = predict_result['cluster_size']
        print "census", census
        pop_spread = sorted(census.values())
        print "counts", pop_spread
        expected_spread = [100, 100, 200]
        squared_spread = sum([(pop_spread[i]-expected_spread[i])**2 for i in range(len(expected_spread))])
        self.assertLess(squared_spread, 200)

    def test_bend_sinister(self):
        """Grid with diagonal division along y = -x."""
        # PCI should ignore the division and cluster according to spatial distances.

        point_list = [(x, y) for x in range(-7, 8) for y in range(-7, 8) if abs(x + y) > 1]
        # 15x15 grid with bend sinister omitted

        predict_result = self.run_normal_case(
            'bend sinister',
            block_data=complete_graph(point_list),
            inspect_count=7,
            k=2)
        out_frame = predict_result['predicted_frame']
        out_frame.add_columns(
            lambda row: (point_list[row.id][0], point_list[row.id][1],
                         point_list[row.id][0] + point_list[row.id][1]),
            [('x', ta.int32), ('y', ta.int32), ('sum', ta.int32)])
        out_frame.sort(['sum', 'cluster', 'id'])
        print out_frame.inspect(out_frame.row_count)
        census = predict_result['cluster_size']
        print "census", census
        pop_spread = sorted(census.values())
        print "counts", pop_spread
        expected_spread = [91, 91]
        squared_spread = sum([(pop_spread[i]-expected_spread[i])**2 for i in range(len(expected_spread))])
        self.assertLess(squared_spread, 40)

    def test_kmeans_quartet(self):
        # Classic k-means test:
        #   4 clusters of 20 points each

        model = ta.PowerIterationClusteringModel()
        edge_frame = frame_utils.build_frame("cluster_kmeans_four_blobs.csv",
                                             self.graph_schema, skip_header_lines=1)
        predict_result = model.predict(edge_frame,
                                       u'Source', u'Destination', u'Similarity',
                                       k=4)

        census = predict_result['cluster_size']
        print "census", census
        pop_spread = sorted(census.values())
        print "counts", pop_spread

        self.assertEquals([20]*4, pop_spread)

    def test_two_arches(self):

        model = ta.PowerIterationClusteringModel()
        edge_frame = frame_utils.build_frame("cluster_two_arches.csv",
                                             self.graph_schema, skip_header_lines=1)
        predict_result = model.predict(edge_frame,
                                       u'Source', u'Destination', u'Similarity',
                                       k=2)

        census = predict_result['cluster_size']
        print "census", census
        pop_spread = sorted(census.values())
        print "counts", pop_spread

        # PIC doesn't handle this case well.
        self.assertEquals([52, 148], pop_spread)
        # self.assertEquals([total/2, total/2], pop_spread)

    def test_mouse(self):
        """Mickey image."""

        model = ta.PowerIterationClusteringModel()
        edge_frame = frame_utils.build_frame("cluster_mouse_head.csv",
                                             self.graph_schema, skip_header_lines=1)
        predict_result = model.predict(edge_frame,
                                       u'Source', u'Destination', u'Similarity',
                                       k=3)
        census = predict_result['cluster_size']
        print "census", census
        pop_spread = sorted(census.values())
        print "counts", pop_spread

        # Check that all region populations are within 30% of expectation,
        #   based on lower value.
        expected = [100, 100, 300]
        for region in range(len(pop_spread)):
            ratio = float(pop_spread[region]) / expected[region]
            self.assertTrue(1.0/1.3 < ratio < 1.3,
                            "FAILURE: expected %d, actual %d" %
                            (expected[region], pop_spread[region]))

    @unittest.skip("Possible error, no bug filed...")
    def test_weird_argument(self):
        """Try various illegal arguments."""

        schema = [
            ('Source', ta.int64),
            ('Destination', ta.int64),
            ('Similarity', ta.float64)
        ]
        model = ta.PowerIterationClusteringModel()

        # Single-edge, 100 clusters -- legal
        edge_frame = frame_utils.build_frame([[1, 2, 1.0]], schema, file_format="list")
        predict_result = model.predict(edge_frame, u'Source', u'Destination', u'Similarity', k=100)
        print predict_result
        print predict_result['predicted_frame'].inspect()

        self.assertEqual(predict_result['number_of_clusters'], 1,
                         "DPNG-4520, returns max instead of actual cluster count")

        # frame = frame_utils.build_frame(block_data, schema, file_format="list")
        # frame.inspect()

    def test_bad_argument(self):
        """Try various illegal arguments."""

        block_data = [
            [1, 2, 1.0],
            [1, 3, 0.3],
            [2, 3, 0.3],
            [3, 0, 0.03],
            [0, 5, 0.01],
            [5, 4, 0.3],
            [5, 6, 1.0],
            [4, 6, 0.3]
        ]
        schema = [
            ('Source', ta.int64),
            ('Destination', ta.int64),
            ('Similarity', ta.float64)
        ]
        model = ta.PowerIterationClusteringModel()

        # Negative distance -- not legal
        bad_frame = frame_utils.build_frame([[1, 2, -1.0]], schema, file_format="list")
        self.assertRaises(ta.rest.command.CommandServerError, model.predict,
                          bad_frame, 'Source', 'Destination', 'Similarity')

        # Skip per DPNG-4521
        # NaN distance -- not legal
        # bad_frame = ta.Frame(ta.UploadRows([[1, 2, np.nan]], schema))
        # self.assertRaises(ta.rest.command.CommandServerError, model.predict,
        #                   bad_frame, 'Source', 'Destination', 'Similarity')

        # Skip per DPNG-4521
        # Extreme distance -- not legal
        # bad_frame = ta.Frame(ta.UploadRows([[1, 2, np.inf]], schema))
        # self.assertRaises(ta.rest.command.CommandServerError, model.predict,
        #                   bad_frame, 'Source', 'Destination', 'Similarity')

        frame = frame_utils.build_frame(block_data, schema, file_format="list")
        frame.inspect()

        # Bad iteration value -- not legal
        self.assertRaises(ta.rest.command.CommandServerError, model.predict,
                          frame, u'Source', u'Destination', u'no_such_column',
                          max_iterations=0)

        # Bad initialization mode -- not legal
        self.assertRaises(ta.rest.command.CommandServerError, model.predict,
                          frame, u'Source', u'Destination', u'no_such_column',
                          initialization_mode=u"no_such_mode")

        # Bad column name -- not legal
        self.assertRaises(ta.rest.command.CommandServerError, model.predict,
                          frame, u'Source', u'Destination', u'no_such_column')

        # Bad k value (must be > 1) -- not legal
        self.assertRaises(ta.rest.command.CommandServerError, model.predict,
                          frame, u'Source', u'Destination', u'Similarity', k=1)

        # Null frame -- not legal
        self.assertRaises(ta.rest.command.CommandServerError, model.predict,
                          None, u'Source', u'Destination', u'Similarity')

        # Empty frame -- not legal
        empty_frame = frame_utils.build_frame([], schema, file_format="list")
        self.assertRaises(ta.rest.command.CommandServerError, model.predict,
                          empty_frame, u'Source', u'Destination', u'Similarity')

        # Single-point frame -- not legal
        point_frame = frame_utils.build_frame([[1, 1, 0.0]], schema, file_format="list")
        self.assertRaises(ta.rest.command.CommandServerError, model.predict,
                          point_frame, u'Source', u'Destination', u'Similarity')

        # Single-edge frame -- legal
        edge_frame = frame_utils.build_frame([[1, 2, 1.0]], schema, file_format="list")
        predict_result = model.predict(edge_frame, u'Source', u'Destination', u'Similarity')


if __name__ == "__main__":
    unittest.main()
