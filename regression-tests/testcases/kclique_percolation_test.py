#############################################################################
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
# ###########################################################################
"""
usage:
python2.7 kclique_percolation_test.py

Tests the kclique functionality, there are known performance issues. Tests
against known results.
"""
__author__ = 'bharadwa'

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import pandas

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class KcliquePercolation(atk_test.ATKTestCase):

    def setUp(self):
        """Build the requisite data frames."""
        super(KcliquePercolation, self).setUp()
        self.clique_graph_data = "cliques_10_10.csv"
        self.noun_graph_data = "noun_graph.csv"

        self.schema = [('from_node', str),
                       ('to_node', str),
                       ('max_k', ia.int64),
                       ('cc', ia.int64)]

        self.schema2 = [("source", str), ("target", str)]
        self.noun_words_frame = frame_utils.build_frame(
            self.noun_graph_data, self.schema2, self.prefix)
        # Big data frame from data with 5 ratings
        self.clique_frame = frame_utils.build_frame(
            self.clique_graph_data, self.schema, self.prefix)

    def test_kclique_for_3_to_10_cliques(self):
        """
        Runs on the dataset created specifically for testing kclique
        algorithm. The dataset has separate cliques of sizes 3 to 10
        and each of those occurs 10 times. Since all the cliques of
        size k also occurs in a clique of size greater than k, we
        can exactly calculate the number of cliques expected. This
        has been used for validation.
        There are 2 separate for loops to avoid running multiple
        gremlin queries.
        """
        graph = ia.Graph()
        graph.define_vertex_type("node")
        graph.vertices["node"].add_vertices(
            self.clique_frame, "from_node", ["max_k", "cc"])
        graph.vertices["node"].add_vertices(
            self.clique_frame, "to_node", ["max_k", "cc"])

        graph.define_edge_type("edge", "node", "node", False)
        graph.edges["edge"].add_edges(
            self.clique_frame, "from_node", "to_node")

        # Executing kclique algorithm for clique size 3 to 10 on
        # odd numbers.
        for k in range(3, 10, 2):
            com = "com_" + str(k)
            print "\nPercolate size", k
            result = graph.kclique_percolation(
                clique_size=k, community_property_label=com)
            frame = result["vertex_dictionary"]["unlabeled"]
            print result
            print frame
            df = frame.download(frame.row_count)
            result_count = len(df[df[com] != ''][com].unique())
            self.assertEqual((10 - (k-1)) * 10, result_count)

    def test_noun_graph_data_parquet(self):
        """
        Runs on the noun graph (graph of connected english noun words).
        The result of clique size 3 has been manually verified on this
        dataset and that is used for validation. This test compares
        number of communities containing a known number of words. Note
        that this is valid for clique size 3 only.

        This runs on parquet
        """
        print "parquet"
        self.noun_graph_result = {3: 528,
                                  4: 102,
                                  5: 35,
                                  6: 12,
                                  7: 7,
                                  8: 5,
                                  9: 2,
                                  10: 2,
                                  11: 3,
                                  13: 1,
                                  14: 2,
                                  17: 1}
        graph = ia.Graph()
        graph.define_vertex_type("source")
        graph.vertices["source"].add_vertices(
            self.noun_words_frame, "source")
        graph.vertices["source"].add_vertices(
            self.noun_words_frame, "target")

        graph.define_edge_type("edge", "source", "source", False)
        graph.edges["edge"].add_edges(
            self.noun_words_frame, "source", "target")
        g = graph

        result = g.kclique_percolation(
            clique_size=3, community_property_label="community")
        frame = result["vertex_dictionary"]["unlabeled"]
        df = frame.download(frame.row_count)
        frame = df[df.community != '']

        cliques = pandas.concat([pandas.Series(row['community'].split(','))
                                 for _, row in frame.iterrows()])

        # Group by the name of the clique, get the size of the clique
        # group by the size of the clique and get the count of cliques
        # of that size
        binned = cliques.groupby(cliques).size()
        count = binned.groupby(binned).size()

        self.assertEqual(count.to_dict(), self.noun_graph_result)


if __name__ == '__main__':
    unittest.main()
