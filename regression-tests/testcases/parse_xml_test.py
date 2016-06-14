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
   Usage:  python2.7 parse_xml.py
   Test XML multi-line parsing.

Functionality tested:
"""

__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2014.12.09.001"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import atk_test
from qalib import config as cf


class XMLReadTest(atk_test.ATKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(XMLReadTest, self).setUp()

        self.dangle_tag_xml = cf.data_location + "/xml_dangle_tag.xml"
        self.overlap_2_xml = cf.data_location + "/xml_overlap_2level.xml"
        self.overlap_inner_xml = cf.data_location + "/xml_overlap_inner.xml"
        self.overlap_xml = cf.data_location + "/xml_overlap.xml"
        self.comment_xml = cf.data_location + "/xml_comment.xml"
        self.doc_xml = cf.data_location + "/xml_doc.xml"
        self.smoke_xml = cf.data_location + "/xml_smoke.xml"

    def _print_xml(self, frame):
        print "Input data:\n"
        for row, node in zip(range(0, 20), frame.take(20)):
            print "Element", row, "\n", node[0], "\n"

    def test_xml_good_001(self):
        """
        Check basic happy-path XML input
        """

        print "\nExercise valid element types"
        xml = ia.XmlFile(self.smoke_xml, "node")
        frame = ia.Frame(xml)

        # for row, node in zip(range(0, 20), frame.take(20)):
        #     print row, node[0], "\n"

        def parse_xml_2col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text, ele.find("value").text

        frame.add_columns(parse_xml_2col, [("name", str), ("value", ia.int32)])

        # Desired parsing is done; it is safe to drop the source XML fragment.
        frame.drop_columns(['data_lines'])
        take = frame.take(20)
        print take
        self.assertEqual(take[2][1], 300, "Node3 value incorrect")
        ia.drop_frames(frame)

    # @unittest.skip("Doesn't handle XML comments\n" +
    #                "https://jira01.devtools.intel.com/browse/TRIB-4223")
    def test_xml_comment(self):
        """
        Check basic happy-path XML input
        """

        print "\nVerify that comments get ignored"
        xml = ia.XmlFile(self.comment_xml, "node")
        frame = ia.Frame(xml)

        self._print_xml(frame)

        def parse_xml_2col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text, ele.find("value").text

        try:
            frame.add_columns(parse_xml_2col, [("name", str),
                                               ("value", ia.int32)])
        except ia.rest.command.CommandServerError as e:
            self.assertNotIn(
                "object has no attribute", str(e),
                "Comment not processed; see " +
                "https://jira01.devtools.intel.com/browse/TRIB-4223")
        except Exception as e:
            print "Identify =>", type(e)
            raise
        else:
            # Desired parsing is done; it is safe to drop the
            # source XML fragment.
            frame.drop_columns(['data_lines'])
            print frame.inspect(20)

        take = frame.take(20)
        print take
        self.assertEqual(take[2][1], 300, "Node3 value incorrect")
        ia.drop_frames(frame)

    def test_xml_square(self):
        """
        Validate the example given in the user documentation.
        """
        # Since this is taken verbatim from the doc,
        #   some lines run over the character limit.

        # Next we create an XmlFile object that defines the file and
        # element tag we are interested in::

        xml_file = ia.XmlFile(self.doc_xml, "square")

        # Now we create a frame using this XmlFile
        frame = ia.Frame(xml_file)

        """
        At this point we have a frame that looks like::
        data_lines
        ------------------------
        '<square>
            <name>left</name>
            <size>3</size>
        </square>'
        '<square color="blue">
            <name>right</name>
            <size>5</size>
        </square>'
        """

        # Now we will want to parse our values out of the xml file.
        # To do this we will use the add_columns method::

        def parse_square_xml(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return (ele.get("color"),
                    ele.find("name").text,
                    ele.find("size").text)

        frame.add_columns(parse_square_xml, [("color", str),
                                             ("name", str),
                                             ("size", str)])
        frame.drop_columns(['data_lines'])
        print frame.inspect(20)

        take = frame.take(20)
        print take
        self.assertEqual(take[0][2], "3", "Square size incorrect")
        ia.drop_frames(frame)

    def test_xml_overlap_outer(self):
        """
        Reject overlapped blocks at top level
        """

        print "\nExercise top-level element overlap"
        xml = ia.XmlFile(self.overlap_xml, "block1")
        frame = ia.Frame(xml)

        self._print_xml(frame)

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        with self.assertRaises(ia.rest.command.CommandServerError):
            frame.add_columns(parse_xml_1col, [("name", str)])

        ia.drop_frames(frame)

    def test_xml_overlap_inner(self):
        """
        Reject overlapped blocks nested within blocks that are otherwise legal.
        """

        print "\nExercise nested-block element overlap"
        xml = ia.XmlFile(self.overlap_inner_xml, "node1")
        frame = ia.Frame(xml)

        self._print_xml(frame)

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        with self.assertRaises(ia.rest.command.CommandServerError):
            frame.add_columns(parse_xml_1col, [("name", str)])

        ia.drop_frames(frame)

    def test_xml_overlap_2level(self):
        """
        Reject overlapped blocks through 2 levels
        """

        print "\nExercise multi-level element overlap"
        xml = ia.XmlFile(self.overlap_2_xml, "block2")
        frame = ia.Frame(xml)

        self._print_xml(frame)

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        with self.assertRaises(ia.rest.command.CommandServerError):
            frame.add_columns(parse_xml_1col, [("name", str)])

        ia.drop_frames(frame)

    def test_xml_dangle(self):
        """
        Accept a partial block.
        """

        print "\nExercise incomplete element cases"
        xml = ia.XmlFile(self.dangle_tag_xml, "node")
        frame = ia.Frame(xml)

        self._print_xml(frame)

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        frame.add_columns(parse_xml_1col, [("name", str)])
        frame.drop_columns(['data_lines'])
        # print "final frame"
        # print frame.inspect(20)

        # take = frame.take(20)
        # print take
        self.assertEqual(frame.row_count, 1, "Row with dangling tag included")

        ia.drop_frames(frame)


if __name__ == "__main__":
    unittest.main()
