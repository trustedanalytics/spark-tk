##############################################################################
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
""" test cases for the kmeans clustering algorithm documentation script
    usage: python2.7 naive_bayes_doc_test.py

    THIS TEST REQUIRES NO THIRD PARTY APPLICATIONS OTHER THAN THE ATK
    THIS TEST IS TO BE MAINTAINED AS A SMOKE TEST FOR THE ML SYSTEM

"""


import unittest
import os

import trustedanalytics as ia


class GraphDocTest(unittest.TestCase):

    def test_graph_example(self):
        """Documentation test for classifiers"""
        # Establish a connection to the ATK Rest Server
        # This handle will be used for the remaineder of the script
        # No cleanup is required

        # First you have to get your server URL and credentials file
        # from your TAP administrator
        atk_server_uri = os.getenv("ATK_SERVER_URI", ia.server.uri)
        credentials_file = os.getenv("ATK_CREDENTIALS", "")

        # set the server, and use the credentials to connect to the ATK
        ia.server.uri = atk_server_uri
        ia.connect(credentials_file)

        # Graphs are composed of 2 sets, one of nodes, and one of edges that
        # connect exactly two (possibly not distinct) nodes. The degree
        # of a node is the number of edges attached to it

        # Below we build a frame using a nodelist and an edgelist.


        node_frame = ia.Frame(
            ia.UploadRows([["node1"],
                           ["node2"],
                           ["node3"],
                           ["node4"],
                           ["node5"]],
                          [("node_name", str)]))
        edge_frame = ia.Frame(
            ia.UploadRows([["node2", "node3"],
                           ["node2", "node1"],
                           ["node2", "node4"],
                           ["node2", "node5"]],
                          [("from", str), ("to", str)]))

        # The graph is a center node on node2, with 4 nodes each attached to 
        # the center node. This is known as a star graph, in this configuration
        # it can be visualized as a plus sign

        # To Create a graph first you define the nodes, and then the edges

        graph = ia.Graph()

        graph.define_vertex_type("node")
        graph.define_edge_type("edge", "node", "node", directed=False)

        graph.vertices["node"].add_vertices(node_frame, "node_name")
        graph.edges["edge"].add_edges(edge_frame, "from", "to") 


        # get the degrees, which have known values
        degrees = graph.annotate_degrees("degree")


        degree_list = degrees["node"].take(5)
        known_list = [[4, u'node', u'node4', 1],
                      [1, u'node', u'node1', 1],
                      [5, u'node', u'node5', 1],
                      [2, u'node', u'node2', 4],
                      [3, u'node', u'node3', 1]]
        for i in known_list:
            self.assertIn(i, degree_list)



if __name__ == '__main__':
    unittest.main()
