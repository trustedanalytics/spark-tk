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

""" Tests the weighted_degree on a graph"""

import unittest

from sparktkregtests.lib import sparktk_test


class WeightedDegreeTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build frames and graphs to be tested"""
        super(WeightedDegreeTest, self).setUp()
        graph_data = self.get_file("clique_10.csv")
        schema = [('src', str),
                  ('dst', str)]

        # set up the vertex frame, which is the union of the src and
        # the dst columns of the edges
        self.frame = self.context.frame.import_csv(graph_data, schema=schema)
        self.vertices = self.frame.copy()
        self.vertices2 = self.frame.copy()
        self.vertices.rename_columns({"src": "id"})
        self.vertices.drop_columns(["dst"])
        self.vertices2.rename_columns({"dst": "id"})
        self.vertices2.drop_columns(["src"])
        self.vertices.append(self.vertices2)
        self.vertices.drop_duplicates()

        self.graph = self.context.graph.create(self.vertices, self.frame)

    def test_degree_out(self):
        """Test degree count for out edges"""
        graph_result = self.graph.degrees(degree_option="out")
        res = graph_result.to_pandas(graph_result.count())
        for _, row in res.iterrows():
            row_val = row['id'].split('_')
            self.assertEqual(int(row_val[2])-1, row['degree'])

    def test_degree_in(self):
        """Test degree count for in edges"""
        graph_result = self.graph.degrees(degree_option="in")
        res = graph_result.to_pandas(graph_result.count())
        for _, row in res.iterrows():
            row_val = row['id'].split('_')
            self.assertEqual(int(row_val[1])-int(row_val[2]), row['degree'])

    def test_degree_undirected(self):
        """Test degree count for undirected edges"""
        graph_result = self.graph.degrees(degree_option="undirected")
        res = graph_result.to_pandas(graph_result.count())
        for _, row in res.iterrows():
            row_val = row['id'].split('_')
            self.assertEqual(int(row_val[1])-1, row['degree'])

    def test_degree_no_name(self):
        """Fails when given no property name"""
        with self.assertRaisesRegexp(Exception, "Invalid degree option"):
            self.graph.degrees("")


if __name__ == "__main__":
    unittest.main()
