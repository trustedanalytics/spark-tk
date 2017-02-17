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

""" Test JSON multi-line parsing.  """

import unittest
import json
import re
import ast
from sparktkregtests.lib import sparktk_test


class JSONReadTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(JSONReadTest, self).setUp()

        self.big_json = self.get_file("json_large.json")
        self.doc_json = self.get_file("json_doc.json")
        self.one_line_json = self.get_file("json_one_line.json")
        self.array_json = self.get_file("json_array.json")
        self.domain_json = self.get_file("domains_1K_lines.json")
        self.over_json = self.get_file("json_over.json")
        self.bad1_json = self.get_file("json_bad1.json")
        self.bad2_json = self.get_file("json_bad2.json")

    def test_json_big(self):
        """ Check basic happy-path XML input """
        frame = self.context.frame.import_json(self.big_json)

        # Desired parsing is done; it is safe to drop the source XML fragment.
        ftake = frame.take(frame.count())
        self.assertTrue("spotweb" in str(ftake[0][0]))
        self.assertTrue("maqetta" in str(ftake[14][0]))

    def test_json_doc(self):
        """Validate the example given in the user documentation."""
        # Since this is taken verbatim from the doc,
        #   some lines run over the character limit.
        # Now we create a frame using this JsonFile
        f = self.context.frame.import_json(self.doc_json)

        ftake = f.take(f.count())
        first_record = ast.literal_eval(str(ftake[0][0]))["obj"]
        self.assertEqual(first_record["color"], "blue")
        second_record = ast.literal_eval(str(ftake[1][0]))["obj"]
        self.assertEqual(second_record["size"], 7)
        third_record = ast.literal_eval(str(ftake[2][0]))["obj"]
        self.assertEqual(third_record["shape"], "square")

    def test_json_one_line(self):
        """Repeat the doc example, but as a one-line JSON file."""
        # Create a frame using this JsonFile
        f = self.context.frame.import_json(self.one_line_json)

        ftake = f.take(f.count())
        first_record = ast.literal_eval(str(ftake[0][0]))["obj"]
        self.assertEqual(first_record["color"], "blue")
        second_record = ast.literal_eval(str(ftake[1][0]))["obj"]
        self.assertEqual(second_record["size"], 7)
        third_record = ast.literal_eval(str(ftake[2][0]))["obj"]
        self.assertEqual(third_record["shape"], "square")

    def test_json_array(self):
        """Repeat the doc example, but as a one-line JSON array."""
        f = self.context.frame.import_json(self.array_json)

        def extract_github_json(row):
            my_json = json.loads(row[0])
            obj = my_json['obj']
            return obj['color'], obj['size'], obj['shape']

        f.add_columns(extract_github_json, [("color", str),
                                            ("size", str),
                                            ("shape", str)])

    def test_json_override(self):
        """ Validate JSON object with duplicate fields """
        jframe = self.context.frame.import_json(self.over_json)

        ftake = jframe.take(jframe.count())
        first_record = ast.literal_eval(str(ftake[0][0]))["obj"]
        self.assertEqual(first_record["color"], "cyan")
        second_record = ast.literal_eval(str(ftake[1][0]))["obj"]
        self.assertEqual(second_record["size"], "6 sqrt 3")

    def test_json_bad_incomplete_record(self):
        """ Validate JSON object with incomplete record at end of file."""
        jframe = self.context.frame.import_json(self.bad1_json)

        ftake = jframe.take(jframe.count())
        self.assertEqual(len(ftake), 2)
        first_record = ast.literal_eval(str(ftake[0][0]))["obj"]
        self.assertEqual(first_record["color"], "cyan")
        second_record = ast.literal_eval(str(ftake[1][0]))["obj"]
        self.assertEqual(second_record["size"], "6 sqrt 3")

    def test_json_bad_extra_close(self):
        """ Validate JSON object with extra closing braces and empty braces"""
        jframe = self.context.frame.import_json(self.bad2_json)

        ftake = jframe.take(jframe.count())
        self.assertEqual(len(ftake), 3)
        second_record = ast.literal_eval(str(ftake[1][0]))["obj"]
        self.assertEqual(second_record["shape"], "triangle")
        third_record = ast.literal_eval(str(ftake[2][0]))["obj"]
        self.assertEqual(third_record["color"], "orange")

    @unittest.skip("sparktk: calling groupby on a frame created from json causes weird errors")
    def test_json_extract(self):
        """Test extracting json code"""

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
                return None, str(e), "BAD0"   # + str(e) + "\n" + str(tb))

        frame_domain = self.context.frame.import_json(self.domain_json)

        frame_domain.add_columns(extract_domain_fields, [("domain", str),
                                                         ("ips", str),
                                                         ("error", str)])

        stats = frame_domain.group_by("error", {"error": self.context.agg.count})
        # don't care about stable order of return
        stats_take = sorted(stats.take(stats.count()))

        good_count = 97
        bad2_count = 901
        bad3_count = 2
        self.assertEqual(stats_take[0][0], None)
        self.assertEqual(stats_take[1][0], "BAD2")
        self.assertEqual(stats_take[2][0], "BAD3")
        self.assertEqual(stats_take[0][1], good_count)
        self.assertEqual(stats_take[1][1], bad2_count)
        self.assertEqual(stats_take[2][1], bad3_count)


if __name__ == "__main__":
    unittest.main()
