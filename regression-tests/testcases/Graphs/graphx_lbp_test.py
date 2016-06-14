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
# ##############################################################################
"""
usage:
python2.7 lbp_graphx_test.py

Tests LBP Graphx implementation by comparing results agains graphlab.

 Command used for generating graphlab LBP posteriors (graphlab version 1.0):
 ./lbp_structured_prediction --prior synth_vdata.tsv --graph synth_edata.tsv
 --output posterior2_vdata.tsv
"""
__author__ = 'bharadwa'

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class LbpGraphx(atk_test.ATKTestCase):

    def setUp(self):
        """Build the graph to analyze"""
        super(LbpGraphx, self).setUp()
        lbp_graphlab_input_data = "torus.csv"

        schema = [("node1", ia.int32),
                  ("node2", ia.int32),
                  ("prior1", str),
                  ("prior2", str)]

        lbp_frame = frame_utils.build_frame(
            lbp_graphlab_input_data, schema, self.prefix)

        lbp_frame.add_columns(lambda row: "1", ("weight", str))
        lbp_frame.add_columns(
            lambda row:
            row['prior1'] + ' ' + row['prior2'], ("sample2", str))
        self.lbp_frame = lbp_frame

        self.torus_graph = ia.Graph()
        self.torus_graph.define_vertex_type("nodes")
        self.torus_graph.vertices["nodes"].add_vertices(
            lbp_frame, "node1", ["sample2"])

        self.torus_graph.define_edge_type(
            "edge", "nodes", "nodes", directed=False)
        self.torus_graph.edges["edge"].add_edges(lbp_frame, "node1", "node2")

    def test_lbp_torus(self):
        """Test the LBP implementation on a torus"""
        result = self.torus_graph.loopy_belief_propagation(
            prior_property="sample2",
            posterior_property="lbp_output",
            max_iterations=10,
            state_space_size=2)
        result_frame = result["vertex_dictionary"]["nodes"]
        df_result = result_frame.download(result_frame.row_count)

        for _, i in df_result.iterrows():
            print i
            str_floats = i['lbp_output']
            print str_floats

            parity = sum(map(int, str(i['node1']))) % 2 == 0
            float1, float2 = map(float, str_floats.split(","))

            self.assertAlmostEqual(float1 + float2, 1, delta=.1)
            if parity:
                self.assertGreater(float1, .9)
                self.assertLess(float2, .1)
            else:
                self.assertGreater(float2, .9)
                self.assertLess(float1, .1)

if __name__ == '__main__':
    unittest.main()
