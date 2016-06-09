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
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test
from qalib import common_utils


class ParquetGraph(atk_test.ATKTestCase):

    def setUp(self):
        """Verify the input and baselines exist before running the tests."""
        super(ParquetGraph, self).setUp()
        self.schema1 = [("col1", str), ("col2", str), ("col3", str)]
        self.schema2 = [("col1a", str), ("col2b", str), ("col3c", str)]

        self.intfile = "int_str_int.csv"

    def test_export_null_edge_attribute(self):
        """ Test exporting a null column on edges"""
        frame1 = frame_utils.build_frame(
            self.intfile, self.schema1, self.prefix)

        frame1.add_columns(lambda x: None, ("nullcol", str))
        graph = ia.Graph()

        graph.define_vertex_type("test_vert")
        graph.define_edge_type("test_edge", "test_vert", "test_vert")

        graph.vertices["test_vert"].add_vertices(frame1, "col1")
        graph.vertices["test_vert"].add_vertices(frame1, "col2")

        graph.edges["test_edge"].add_edges(frame1, "col1", "col2", ["nullcol"])

        self.assertEqual(graph.vertex_count, 6)

    def test_re_adding_attributes(self):
        """ Test exporting a null column on edges"""
        frame1 = frame_utils.build_frame(
            self.intfile, self.schema1, self.prefix)

        graph = ia.Graph()

        graph.define_vertex_type("test_vert")
        graph.define_edge_type("test_edge", "test_vert", "test_vert")

        graph.vertices["test_vert"].add_vertices(frame1, "col1", ["col2"])
        graph.vertices["test_vert"].add_vertices(frame1, "col1", ["col3"])

        result = graph.vertices["test_vert"].take(20).sort()
        baseline = [[4, u'test_vert', u'1', None, u'2'],
                    [5, u'test_vert', u'2', None, u'4'],
                    [6, u'test_vert', u'3', None, u'6']].sort()

        self.assertEqual(result, baseline)


if __name__ == '__main__':
    unittest.main()
