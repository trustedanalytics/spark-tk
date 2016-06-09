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
"""
   Usage:  python2.7 drop_unified_test.py
   Test interface functionality of the unified drop method.
"""
__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2015.10.29"


# Functionality tested:
#   NOTE: original function is still covered by test cases for
#       drop_frames, drop_graphs, and drop_models.
#
# mixture of frames, graphs, and models
# mixture of names and handles
# mixture of valid and invalid names
# mixture of valid and invalid objects
# unsupported data type (integer will do)

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ta

from qalib import common_utils
from qalib import atk_test


class UnifiedDropTest(atk_test.ATKTestCase):

    def setUp(self):
        """create standard frames for general use"""
        super(UnifiedDropTest, self).setUp()

    def test_drop_name_mixture(self):
        """Drop names of a mixture of models, graphs, and frames."""

        f1 = ta.Frame(name=common_utils.get_a_name(self.prefix))
        f2 = ta.Frame(name=common_utils.get_a_name(self.prefix))
        g1 = ta.Graph(name=common_utils.get_a_name(self.prefix))
        g2 = ta.Graph(name=common_utils.get_a_name(self.prefix))
        m1 = ta.KMeansModel(name=common_utils.get_a_name(self.prefix))
        m2 = ta.SvmModel(name=common_utils.get_a_name(self.prefix))
        m3 = ta.LdaModel(name=common_utils.get_a_name(self.prefix))

        plumb_list = [m1, g1, f1, m2, g2, f2, m3]

        drop_pop = ta.drop([_.name for _ in plumb_list])
        self.assertEqual(len(plumb_list), drop_pop)

    def test_drop_handle_mixture(self):
        """Drop a mixture of models, graphs, and frames,
           both names and handles."""

        f1 = ta.Frame(name=common_utils.get_a_name(self.prefix))
        f2 = ta.Frame(name=common_utils.get_a_name(self.prefix))
        g1 = ta.Graph(name=common_utils.get_a_name(self.prefix))
        g2 = ta.Graph(name=common_utils.get_a_name(self.prefix))
        m1 = ta.KMeansModel(name=common_utils.get_a_name(self.prefix))
        m2 = ta.SvmModel(name=common_utils.get_a_name(self.prefix))
        m3 = ta.LdaModel(name=common_utils.get_a_name(self.prefix))

        plumb_list = [m1, g1, f1, m2, g2, f2, m3]

        drop_pop = ta.drop(plumb_list)
        self.assertEqual(len(plumb_list), drop_pop)

    def test_drop_type_mixture(self):
        """Drop a mixture of names and handles."""

        f1 = ta.Frame(name=common_utils.get_a_name(self.prefix))
        f2 = ta.Frame(name=common_utils.get_a_name(self.prefix))
        g1 = ta.Graph(name=common_utils.get_a_name(self.prefix))
        g2 = ta.Graph(name=common_utils.get_a_name(self.prefix))
        m1 = ta.KMeansModel(name=common_utils.get_a_name(self.prefix))
        m2 = ta.SvmModel(name=common_utils.get_a_name(self.prefix))
        m3 = ta.LdaModel(name=common_utils.get_a_name(self.prefix))

        plumb_list = [m1.name, g1, f1.name, m2, g2.name, f2, m3]

        drop_pop = ta.drop(plumb_list)
        self.assertEqual(len(plumb_list), drop_pop)

    def test_drop_bad_name(self):
        """Test a mixture of valid and invalid names."""
        # Include invalid names: spaces, special chars, etc.

        f1 = ta.Frame(name=common_utils.get_a_name(self.prefix))
        g1 = ta.Graph(name=common_utils.get_a_name(self.prefix))
        m1 = ta.KMeansModel(name=common_utils.get_a_name(self.prefix))

        plumb_list = [m1.name, g1.name, f1.name]
        expected = len(plumb_list)
        plumb_list.insert(2, "No_Such Item")
        plumb_list.insert(1, "badFrameName")

        drop_pop = ta.drop(plumb_list)
        self.assertEqual(expected, drop_pop)

    def test_drop_bad_type(self):
        """Drop a mixture of valid handles and other stuff."""

        # Drop an unsupported type: integer

        f1 = ta.Frame(name=common_utils.get_a_name(self.prefix))
        g1 = ta.Graph(name=common_utils.get_a_name(self.prefix))
        m1 = ta.KMeansModel(name=common_utils.get_a_name(self.prefix))
        data_source = ta.UploadRows([[1, 1], [2, 2]],
                                    [ta.int32, ta.int32])

        plumb_list = [m1, "dummy", g1, f1]
        expected = len(plumb_list)-1

        # Try to drop a data source, an integer, None, and an invalid name
        plumb_list[1] = data_source
        self.assertRaises(AttributeError, ta.drop, plumb_list)

        # The exception will abort the drop after the first element; reset
        plumb_list[0] = ta.KMeansModel(name="m1")
        plumb_list[1] = 42
        self.assertRaises(AttributeError, ta.drop, plumb_list)

        plumb_list[0] = ta.KMeansModel(name="m1")
        plumb_list[1] = None
        self.assertEqual(expected, ta.drop(plumb_list))

        # "None" didn't interrupt anything, so rebuild the list.
        f1 = ta.Frame(name=common_utils.get_a_name(self.prefix))
        g1 = ta.Graph(name=common_utils.get_a_name(self.prefix))
        m1 = ta.KMeansModel(name=common_utils.get_a_name(self.prefix))
        plumb_list = [m1, "dummy", g1, f1]
        expected = len(plumb_list)-1
        self.assertEqual(expected, ta.drop(plumb_list))

    def test_drop_edge_cases(self):
        """Edge cases of the new routine's interface."""
        # empty list
        # null series
        # single name
        # single handle
        # duplicate drop
        # list of one item
        # series of items (not a list; comma-separated)
        # large list

        # empty list
        drop_pop = ta.drop([])
        self.assertEqual(0, drop_pop)

        # null series
        drop_pop = ta.drop()
        self.assertEqual(0, drop_pop)

        # single name
        frame = ta.Frame(name=common_utils.get_a_name(self.prefix))
        drop_pop = ta.drop(frame.name)
        self.assertEqual(1, drop_pop)

        # single handle
        frame = ta.Frame()
        drop_pop = ta.drop(frame)
        self.assertEqual(1, drop_pop)

        # list of one item
        frame = ta.Frame()
        drop_pop = ta.drop([frame])
        self.assertEqual(1, drop_pop)

        # duplicate drop: list
        frame = ta.Frame()
        graph = ta.Graph()
        drop_pop = ta.drop([frame, graph, frame])
        self.assertEqual(2, drop_pop)

        # duplicate drop: series, handles
        frame = ta.Frame()
        graph = ta.Graph()
        drop_pop = ta.drop(graph, frame, frame)
        self.assertEqual(2, drop_pop)

        # duplicate drop: both name and handle
        frame = ta.Frame(name=common_utils.get_a_name(self.prefix))
        graph = ta.Graph()
        drop_pop = ta.drop(graph, frame.name, frame)
        self.assertEqual(2, drop_pop)

        # large list
        limit = 100
        plumb_list = []
        for _ in range(limit):
            plumb_list.append(ta.Frame())
        drop_pop = ta.drop(plumb_list)
        self.assertEqual(limit, drop_pop)

    def test_same_name(self):
        """Side effect: frame/graph/model cannot have same name."""
        # Try in all six pair permutations
        # TODO: When DPAT-943 is fixed, change to a single exception type.

        reuse = common_utils.get_a_name("Overload")

        ta.Frame(name=reuse)
        self.assertRaises(RuntimeError, ta.Graph, name=reuse)
        self.assertRaises(
            ta.rest.command.CommandServerError, ta.KMeansModel, name=reuse)
        ta.drop(reuse)

        ta.Graph(name=reuse)
        self.assertRaises(RuntimeError, ta.Frame, name=reuse)
        self.assertRaises(
            ta.rest.command.CommandServerError, ta.KMeansModel, name=reuse)
        ta.drop(reuse)

        ta.KMeansModel(name=reuse)
        self.assertRaises(RuntimeError, ta.Graph, name=reuse)
        self.assertRaises(RuntimeError, ta.Frame, name=reuse)
        ta.drop(reuse)


if __name__ == "__main__":
    unittest.main()
