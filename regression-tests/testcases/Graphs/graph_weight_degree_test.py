# #############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
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
Usage: python2.7 annotation_graph_algorithm.py
Tests the annotate_degree and annotate_weighted_degree on a graph.
"""

# TODO: LCOATESX describe the graph that is ued in the test case

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class GraphWeightedDegreeTest(atk_test.ATKTestCase):

    def setUp(self):
        """Build frames and graphs to be tested."""
        super(GraphWeightedDegreeTest, self).setUp()
        schema_node = [("nodename", str),
                       ("in", ia.int64),
                       ("out", ia.int64),
                       ("undirectedcount", ia.int64),
                       ("isundirected", ia.int64),
                       ("outlabel", ia.int64),
                       ("insum", ia.float64),
                       ("outsum", ia.float64),
                       ("undirectedsum", ia.float64),
                       ("labelsum", ia.float64),
                       ("nolabelsum", ia.float64),
                       ("defaultsum", ia.float64),
                       ("integersum", ia.int64)]

        schema_directed = [("nodefrom", str),
                           ("nodeto", str),
                           ("value", ia.float64),
                           ("badvalue", str),
                           ("intvalue", ia.int32),
                           ("int64value", ia.int64)]

        schema_undirected = [("node1", str),
                             ("node2", str),
                             ("value", ia.float64)]

        schema_directed_label = [("nodefrom", str),
                                 ("nodeto", str),
                                 ("labeltest", ia.float64)]

        # Build the frames for the graph
        node_frame = frame_utils.build_frame(
            "annotate_node_list.csv", schema_node)
        node_directed = node_frame.copy()
        node_directed.filter(lambda x: x["isundirected"] == 0)

        node_undirected = node_frame.copy()
        node_undirected.filter(lambda x: x["isundirected"] == 1)

        directed_frame = frame_utils.build_frame(
            "annotate_directed_list.csv", schema_directed)
        undirected_frame = frame_utils.build_frame(
            "annotate_undirected_list.csv", schema_undirected)
        directed_label_frame = frame_utils.build_frame(
            "annotate_directed_label_list.csv", schema_directed_label)

        # Build the graph
        graph = ia.Graph()

        # add the frames to the graph
        graph.define_vertex_type("directed")

        graph.vertices['directed'].add_vertices(
            node_directed,
            "nodename",
            ["out",
             "undirectedcount",
             "isundirected",
             "outlabel",
             "in",
             "insum",
             "outsum",
             "undirectedsum",
             "labelsum",
             "nolabelsum",
             "defaultsum",
             "integersum"])

        graph.define_vertex_type("undirected")

        graph.vertices['undirected'].add_vertices(
            node_undirected,
            "nodename",
            ["out",
             "undirectedcount",
             "isundirected",
             "outlabel",
             "in",
             "insum",
             "outsum",
             "undirectedsum",
             "labelsum",
             "nolabelsum",
             "defaultsum",
             "integersum"])

        graph.define_edge_type("directededges", "directed", "directed",
                               directed=True)
        graph.define_edge_type("labeldirected", "directed", "directed",
                               directed=True)
        graph.define_edge_type("undirectededges", "undirected", "undirected",
                               directed=False)

        graph.edges['directededges'].add_edges(
            directed_frame, "nodefrom",
            "nodeto", ["value",
                       "badvalue",
                       "intvalue",
                       "int64value"])
        graph.edges['labeldirected'].add_edges(directed_label_frame,
                                               "nodefrom", "nodeto",
                                               ["labeltest"])
        graph.edges['undirectededges'].add_edges(
            undirected_frame, "node1", "node2", ["value"])

        self.graph = graph

    def test_annotate_node_degree_out(self):
        """Test degree count annotation."""
        # Build the annotation on all edges
        graph_result = self.graph.annotate_degrees(
            "countName", degree_option="out")
        self._verify_frame(graph_result['directed'], "countName", "out")

    def test_annotate_node_degree_in(self):
        """Test degree count annotation for in edges."""
        # annotate graph using only in count
        graph_result = self.graph.annotate_degrees(
            "countName", degree_option="in")
        self._verify_frame(graph_result['directed'], "countName", "in")

    def test_annotate_node_degree_undirected(self):
        """Test degree count annotation for undirected edges."""
        # annotate graph using only undirected edges
        graph_result = self.graph.annotate_degrees(
            "countName", degree_option="undi")
        self._verify_frame(
            graph_result['undirected'], "countName", "undirectedcount")

    def test_annotate_node_degree_labeled_edges_only(self):
        """Test degree count annotation on specially labeled edges."""
        # annotate graph using only specially labeled edges
        graph_result = self.graph.annotate_degrees(
            "countName", input_edge_labels=["labeldirected"])
        self._verify_frame(graph_result['directed'], "countName", "outlabel")

    def test_annotate_weight_degree_in(self):
        """Test degree count annotation weighted on in edges."""
        # sum weighted edges on in edges
        annotated_dict = self.graph.annotate_weighted_degrees(
            "sumName", degree_option="in", edge_weight_property="value")
        self._verify_frame(
            annotated_dict["directed"], "sumName", "insum")

    def test_annotate_weight_degree_out(self):
        """Test degree count annotation weighted on out edges."""
        # sum weighted edges on in edges
        annotated_dict = self.graph.annotate_weighted_degrees(
            "sumName", edge_weight_property="value")
        self._verify_frame(
            annotated_dict["directed"], "sumName", "outsum")

    def test_annotate_weight_degree_undirected(self):
        """Test degree count annotation weighted on undirected edges."""
        # sum weighted edges on in edges
        annotated_dict = self.graph.annotate_weighted_degrees(
            "sumName", edge_weight_property="value", degree_option="undi")
        self._verify_frame(
            annotated_dict["undirected"], "sumName", "undirectedsum")

    def test_annotate_weight_degree_label(self):
        """Test degree count annotation weighted on specially labeled edges."""
        annotated_dict = self.graph.annotate_weighted_degrees(
            "sumName", edge_weight_property="labeltest",
            input_edge_labels=["labeldirected"])
        self._verify_frame(
            annotated_dict["directed"], "sumName", "labelsum")

    def test_annotate_weight_no_value(self):
        """Test degree count annotation weighted with no values."""
        annotated_dict = self.graph.annotate_weighted_degrees("sumName")
        self._verify_frame(
            annotated_dict["undirected"], "sumName", "nolabelsum")

    def test_annotate_weight_default(self):
        """Test degree count annotation weighted with default value."""
        annotated_dict = self.graph.annotate_weighted_degrees(
            "sumName", edge_weight_property="value", edge_weight_default=0.5)
        self._verify_frame(
            annotated_dict["directed"], "sumName", "defaultsum")

    def test_annotate_weight_integer(self):
        """Test degree count annotation weighted with default value."""
        annotated_dict = self.graph.annotate_weighted_degrees(
            "sumName", edge_weight_property="intvalue",
            degree_option="in", edge_weight_default=0)
        self._verify_frame(
            annotated_dict["directed"], "sumName", "integersum")

    def test_annotate_weight_integer64(self):
        """Test degree count annotation weighted with default value."""
        annotated_dict = self.graph.annotate_weighted_degrees(
            "sumName", edge_weight_property="int64value",
            degree_option="in", edge_weight_default=0)
        self._verify_frame(
            annotated_dict["directed"], "sumName", "integersum")

    def test_annotate_weight_type_error(self):
        """Test degree count annotation weighted with type error."""
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.graph.annotate_weighted_degrees,
                          "sumName",
                          edge_weight_property="badvalue")

    # @unittest.skip("https://intel-data.atlassian.net/browse/DPAT-334")
    def test_annotate_weight_non_value(self):
        """Test degree count annotation weighted with type error."""
        graph_name = common_utils.get_a_name(self.prefix)
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.graph.annotate_weighted_degrees,
                          graph_name, "sumName",
                          edge_weight_property="nonvalue")

    def test_annotate_node_degree_no_name(self):
        """Fails when given no property name."""
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.graph.annotate_degrees, "")

    def test_annotate_node_weight_degree_no_name(self):
        """Fails when given no property name."""
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.graph.annotate_weighted_degrees, "")

    def _verify_frame(self, frame, first, second):
        # isundirected is either a 0 or a 1, booleans are not supported
        # by the atk
        print frame.inspect()
        result = frame.download(frame.row_count)

        for _, i in result.iterrows():
            self.assertEqual(i[first], i[second])

if __name__ == "__main__":
    unittest.main()
