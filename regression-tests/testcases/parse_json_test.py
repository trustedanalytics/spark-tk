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
   Usage:  python2.7 parse_json.py
   Test JSON multi-line parsing.

Functionality tested:
  Positive:
    Implement the example from the documentation
    Parse simple and complex records
    Skip uninteresting records
    Extract desired fields
  Negative:
    Duplicate field
    Incomplete record
    Extra record closing
"""

__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2014.12.17.001"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import json
import re

import trustedanalytics as ia

from qalib import config as cf
from qalib import atk_test


class JSONReadTest(atk_test.ATKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(JSONReadTest, self).setUp()

        self.big_json = cf.data_location + "/json_large.json"
        self.doc_json = cf.data_location + "/json_doc.json"
        self.one_line_json = cf.data_location + "/json_one_line.json"
        self.array_json = cf.data_location + "/json_array.json"
        self.domain_json = cf.data_location + "/domains_1K_lines.json"
        self.over_json = cf.data_location + "/json_over.json"
        self.bad1_json = cf.data_location + "/json_bad1.json"
        self.bad2_json = cf.data_location + "/json_bad2.json"
        self.kyle_15M = cf.data_location + "/psRawTS_inp_2019-01-21.json"

    def _print_xml(self, frame, row_limit=20):
        print "Input data:\n"
        frame_take = frame.take(row_limit)
        row_count = len(frame_take)
        print row_count, "rows in frame"
        for row, node in zip(range(0, row_count), frame_take):
            print "Element", row, "\n", node[0], "\n"

    def test_json_big(self):
        """
        Check basic happy-path XML input
        This runs on a modified copy of
        http://data.githubarchive.org/2012-04-11-15.json.gz, a file of
        just over 6Mb.  There are several dozen invalid records.
        """

        print "\nExercise 6Mb file"
        json_file = ia.JsonFile(self.big_json)
        f = ia.Frame(json_file)
        # print "before parsing:\n", f.inspect(20)

        # for row, node in zip(range(0,frame.row_count), frame.take(20)):
        #     print row, node[0], "\n"

        def extract_github_json(row):
            """
            Return the listed fields from the file.
            Return 'None' for any missing element.
            """

            my_json = json.loads(row[0])
            try:
                obj = my_json['repository']
            except Exception:
                name, url, desc = None, None, None
            else:
                try:
                    name = obj['name']
                except Exception:
                    name = None
                try:
                    url = obj['url']
                except Exception:
                    url = None
                try:
                    desc = obj['description']
                except Exception:
                    desc = None
            return name, url, desc

        # ftake = f.take(10000)
        # i = 0
        # for my_line in ftake:
        #     i += 1
        #     try:
        #         a, b, c = extract_github_json(my_line)
        #     except Exception as e:
        #         print "Failed on line", i, my_line, "with exception", type(e)
        #         print "name:", a, "url:", b, "description:", c
            # print "record ", i, a, b, c

        f.add_columns(extract_github_json,
                      [("name", str), ("url", str), ("description", str)])
        # self._print_xml(f)

        # Desired parsing is done; it is safe to drop the source XML fragment.
        f.drop_columns(['data_lines'])
        print "See input data:\n", f.inspect(20)
        ftake = f.take(f.row_count)
        self.assertEqual(ftake[0][0], "spotweb")
        self.assertEqual(ftake[14][2], "Maqetta Designer")

    def test_json_doc(self):
        """Validate the example given in the user documentation."""
        # Since this is taken verbatim from the doc,
        #   some lines run over the character limit.

        # Next we create a JsonFile object that defines the file we
        # are interested in::

        # Now we create a frame using this JsonFile
        json_file = ia.JsonFile(self.doc_json)
        f = ia.Frame(json_file)
        self._print_xml(f)

        # At this point we have a frame that looks like::
        # data_lines
        # ------------------------
        # '{ "obj": {
        #       "color": "blue",
        #       "size": 3,
        #       "shape": "square" }
        # }'
        #  '{ "obj": {
        #       "color": "green",
        #       "size": 7,
        #       "shape": "triangle" }
        # }'
        #  '{ "obj": {
        #       "color": "orange",
        #       "size": 10,
        #       "shape": "square" }
        # }'
        #
        # Now we will want to parse our values out of the xml file. To do
        # this we will use the add_columns method::

        def extract_github_json(row):
            my_json = json.loads(row[0])
            obj = my_json['obj']
            return obj['color'], obj['size'], obj['shape']

        f.add_columns(extract_github_json, [("color", str),
                                            ("size", str),
                                            ("shape", str)])

        # Desired parsing is done; it is safe to drop the source XML fragment.
        f.drop_columns(['data_lines'])

        # This produces a frame that looks like::
        # color       size        shape
        # ---------   ----------  ----------
        # blue        3           square
        # green       7           triangle
        # orange      10          square

        ftake = f.take(f.row_count)
        self.assertEqual(ftake[0][0], "blue")
        self.assertEqual(ftake[1][1], "7")
        self.assertEqual(ftake[2][2], "square")

    # @unittest.skip("Data file not yet in place")
    def test_json_one_line(self):
        """Repeat the doc example, but as a one-line JSON file."""

        # Create a frame using this JsonFile
        json_file = ia.JsonFile(self.one_line_json)
        f = ia.Frame(json_file)
        self._print_xml(f)

        # Now we will want to parse our values out of the xml file. To do
        # this we will use the add_columns method::

        def extract_github_json(row):
            my_json = json.loads(row[0])
            obj = my_json['obj']
            return obj['color'], obj['size'], obj['shape']

        f.add_columns(extract_github_json, [("color", str),
                                            ("size", str),
                                            ("shape", str)])

        # Desired parsing is done; it is safe to drop the source XML fragment.
        f.drop_columns(['data_lines'])

        ftake = f.take(f.row_count)
        self.assertEqual(ftake[0][0], "blue")
        self.assertEqual(ftake[1][1], "7")
        self.assertEqual(ftake[2][2], "square")

    # @unittest.skip("Data file not yet in place")
    def test_json_array(self):
        """Repeat the doc example, but as a one-line JSON array."""

        # Create a frame using this JsonFile
        json_file = ia.JsonFile(self.array_json)
        f = ia.Frame(json_file)
        self._print_xml(f)

        # Now we will want to parse our values out of the xml file. To do
        # this we will use the add_columns method::

        def extract_github_json(row):
            my_json = json.loads(row[0])
            obj = my_json['obj']
            return obj['color'], obj['size'], obj['shape']

        f.add_columns(extract_github_json, [("color", str),
                                            ("size", str),
                                            ("shape", str)])

        # Desired parsing is done; it is safe to drop the source XML fragment.
        f.drop_columns(['data_lines'])

    def test_json_override(self):
        """
        Validate JSON object with duplicate fields.
        Second appearance overrides the first
        """
        json_file = ia.JsonFile(self.over_json)
        jframe = ia.Frame(json_file)
        # self._print_xml(jframe)

        def extract_github_json(row):
            my_json = json.loads(row[0])
            obj = my_json['obj']
            return obj['color'], obj['size'], obj['shape']

        jframe.add_columns(extract_github_json, [("color", str),
                                                 ("size", str),
                                                 ("shape", str)])

        # Now that we have parsed our desired values it is safe to
        # drop the source XML fragment::
        jframe.drop_columns(['data_lines'])
        print "See input data:\n", jframe.inspect(20)
        ftake = jframe.take(jframe.row_count)
        self.assertEqual(ftake[0][0], "cyan")
        self.assertEqual(ftake[1][1], "6 sqrt 3")

        # This produces a frame that looks like::
        # color       size        shape
        # ---------   ----------  ----------
        # cyan        3           square
        # green       6 sqrt 3    triangle
        # orange      10          square

    def test_json_bad_incomplete_record(self):
        """
        Validate JSON object with incomplete record at end of file.
        Final record should be ignored.
        """
        json_file = ia.JsonFile(self.bad1_json)
        jframe = ia.Frame(json_file)
        # self._print_xml(f)

        def extract_github_json(row):
            my_json = json.loads(row[0])
            obj = my_json['obj']
            return obj['color'], obj['size'], obj['shape']

        jframe.add_columns(extract_github_json, [("color", str),
                                                 ("size", str),
                                                 ("shape", str)])

        # Now that we have parsed our desired values it is safe to drop
        # the source XML fragment::
        jframe.drop_columns(['data_lines'])
        print "See input data:\n", jframe.inspect(20)
        ftake = jframe.take(jframe.row_count)
        self.assertEqual(len(ftake), 2)
        self.assertEqual(ftake[0][0], "cyan")
        self.assertEqual(ftake[1][1], "6 sqrt 3")

        # This produces a frame that looks like::
        # color       size        shape
        # ---------   ----------  ----------
        # cyan        3           square
        # green       6 sqrt 3    triangle

    def test_json_bad_extra_close(self):
        """
        Validate JSON object with extra closing braces and empty braces.
        Both conditions should be ignored.
        """
        json_file = ia.JsonFile(self.bad2_json)
        jframe = ia.Frame(json_file)
        # self._print_xml(f)

        def extract_github_json(row):
            my_json = json.loads(row[0])
            obj = my_json['obj']
            return obj['color'], obj['size'], obj['shape']

        jframe.add_columns(extract_github_json, [("color", str),
                                                 ("size", str),
                                                 ("shape", str)])

        # Now that we have parsed our desired values it is safe
        # to drop the source XML fragment::
        jframe.drop_columns(['data_lines'])
        print "See input data:\n", jframe.inspect(20)
        ftake = jframe.take(jframe.row_count)
        self.assertEqual(len(ftake), 3)
        self.assertEqual(ftake[1][2], "triangle")
        self.assertEqual(ftake[2][0], "orange")

        # This produces a frame that looks like::
        # color       size        shape
        # ---------   ----------  ----------
        # cyan        3           square
        # green       6 sqrt 3    triangle
        # orange      10          square

    def test_json_extract(self):

        def extract_domain_fields(row):
            """
            Split domain lines into 3 columns: (domain, ips, error)
            This is a more robust and complicated parse than the above.
            """
            try:
                line = row[0].encode('utf-8').strip()
                if line:
                    # see if we can close over this compiled date_pattern :^)
                    date_pattern = re.compile("Date\(\s*(\d+)\s*\)")
                    ipaddr_pattern = \
                        re.compile("\d\d?\d?\.\d\d?\d?\.\d\d?\d?\.\d\d?\d?")
                    line = date_pattern.sub("\\1", line)

                    d = json.loads(line)
                    domain = d['_id'].lower()
                    if ipaddr_pattern.match(domain):
                        return None, None, "BAD1"
                    try:
                        ip_data = d['ip_data']
                    except KeyError:
                        return None, None, "BAD2"
                    addrs = []
                    for table in ip_data:
                        ipaddr = table['_id']
                        if not ipaddr_pattern.match(ipaddr):
                            return None, None, "BAD3"
                        addrs.append(ipaddr)
                    ips = ",".join(addrs)
                    return domain, ips, None
                else:
                    return None, None, None
            except Exception as e:
                # tb = traceback.format_exc()
                return None, str(e), "BAD0"   # + str(e) + "\n" + str(tb))

        print "\n\t Inside get_domains_frame here! \n"

        line_file = ia.LineFile(self.domain_json)
        frame_domain = ia.Frame(line_file)
        print "Just created the frame."
        print frame_domain.inspect(2)

        frame_domain.add_columns(extract_domain_fields, [("domain", str),
                                                         ("ips", str),
                                                         ("error", str)])

        stats = frame_domain.group_by("error", {"error": ia.agg.count})
        # don't care about stable order of return
        stats_take = sorted(stats.take(stats.row_count))
        print stats_take

        good_count = 97
        bad2_count = 901
        bad3_count = 2
        self.assertEqual(stats_take[0][0], None)
        self.assertEqual(stats_take[1][0], "BAD2")
        self.assertEqual(stats_take[2][0], "BAD3")
        self.assertEqual(stats_take[0][1], good_count)
        self.assertEqual(stats_take[1][1], bad2_count)
        self.assertEqual(stats_take[2][1], bad3_count)

        ia.drop_frames([stats, frame_domain])


if __name__ == "__main__":
    unittest.main()
