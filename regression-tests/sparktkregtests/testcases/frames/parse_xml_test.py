# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

""" Test XML multi-line parsing.  """

import unittest

import trustedanalytics as ia

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from qalib import atk_test
from qalib import common_utils as cu


class XMLReadTest(atk_test.ATKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(XMLReadTest, self).setUp()

        self.dangle_tag_xml = cu.get_file("xml_dangle_tag.xml")
        self.overlap_2_xml = cu.get_file("xml_overlap_2level.xml")
        self.overlap_inner_xml = cu.get_file("xml_overlap_inner.xml")
        self.overlap_xml = cu.get_file("xml_overlap.xml")
        self.comment_xml = cu.get_file("xml_comment.xml")
        self.doc_xml = cu.get_file("xml_doc.xml")
        self.smoke_xml = cu.get_file("xml_smoke.xml")

    def test_xml_good_001(self):
        """ Check basic happy-path XML input """

        print "\nExercise valid element types"
        xml = ia.XmlFile(self.smoke_xml, "node")
        frame = ia.Frame(xml)

        def parse_xml_2col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text, ele.find("value").text

        frame.add_columns(parse_xml_2col, [("name", str), ("value", ia.int32)])

        # Desired parsing is done; it is safe to drop the source XML fragment.
        frame.drop_columns(['data_lines'])
        take = frame.take(20)
        self.assertEqual(take[2][1], 300, "Node3 value incorrect")
        ia.drop_frames(frame)

    def test_xml_comment(self):
        """ Check basic happy-path XML input """

        xml = ia.XmlFile(self.comment_xml, "node")
        frame = ia.Frame(xml)

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

        take = frame.take(20)
        self.assertEqual(take[2][1], 300, "Node3 value incorrect")
        ia.drop_frames(frame)

    def test_xml_square(self):
        """ Validate the example given in the user documentation.  """
        # Since this is taken verbatim from the doc,
        #   some lines run over the character limit.

        # Next we create an XmlFile object that defines the file and
        # element tag we are interested in::

        xml_file = ia.XmlFile(self.doc_xml, "square")

        # Now we create a frame using this XmlFile
        frame = ia.Frame(xml_file)

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

        take = frame.take(20)
        self.assertEqual(take[0][2], "3", "Square size incorrect")
        ia.drop_frames(frame)

    def test_xml_overlap_outer(self):
        """ Reject overlapped blocks at top level """

        xml = ia.XmlFile(self.overlap_xml, "block1")
        frame = ia.Frame(xml)

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        with self.assertRaises(ia.rest.command.CommandServerError):
            frame.add_columns(parse_xml_1col, [("name", str)])

        ia.drop_frames(frame)

    def test_xml_overlap_inner(self):
        """Reject overlapped blocks nested within blocks, otherwise legal"""

        xml = ia.XmlFile(self.overlap_inner_xml, "node1")
        frame = ia.Frame(xml)

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        with self.assertRaises(ia.rest.command.CommandServerError):
            frame.add_columns(parse_xml_1col, [("name", str)])

        ia.drop_frames(frame)

    def test_xml_overlap_2level(self):
        """ Reject overlapped blocks through 2 levels"""

        xml = ia.XmlFile(self.overlap_2_xml, "block2")
        frame = ia.Frame(xml)

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        with self.assertRaises(ia.rest.command.CommandServerError):
            frame.add_columns(parse_xml_1col, [("name", str)])

        ia.drop_frames(frame)

    def test_xml_dangle(self):
        """ Accept a partial block.  """
        xml = ia.XmlFile(self.dangle_tag_xml, "node")
        frame = ia.Frame(xml)

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        frame.add_columns(parse_xml_1col, [("name", str)])
        frame.drop_columns(['data_lines'])
        self.assertEqual(frame.row_count, 1)

        ia.drop_frames(frame)


if __name__ == "__main__":
    unittest.main()
