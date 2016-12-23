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

"""Test column naming and quantity of joins"""
import unittest
import sys
import os
from sparktkregtests.lib import sparktk_test
from sparktk import dtypes
from py4j.protocol import Py4JJavaError


class JoinTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Set up frames to be build """
        super(JoinTest, self).setUp()
        block_data = [[0, "sifuri"],
                      [1, "moja"],
                      [2, "mbili"],
                      [3, "tatu"],
                      [4, "nne"],
                      [5, "tano"]]

        right_data = [[6, "sita"],
                      [7, "saba"],
                      [8, "nane"],
                      [9, "tisa"],
                      [10, "kumi"]]

        schema = [("idnum", int), ("count", str)]

        self.frame = self.context.frame.create(data=block_data, schema=schema)
        self.right_frame = self.context.frame.create(data=right_data,
                                                     schema=schema)
        self.empty_frame = self.context.frame.create(data=[], schema=schema)

    def test_name_collision(self):
        """Test joining repeatedly doesn't collide """
        # Repeatedly run join to force collisions
        frame = self.frame.join_inner(
            self.frame, left_on="idnum", right_on="idnum")
        frame = frame.join_inner(
            self.frame, left_on="idnum", right_on="idnum")
        frame = frame.join_inner(
            self.frame, left_on="idnum", right_on="idnum")
        baseline = [u'idnum', u'count_L', u'count_R',
                    u'count_L_L', u'count_R_R']

        self.assertItemsEqual(frame.column_names, baseline)

    def test_type_int32(self):
        """Test join on int32"""
        joined_frame = self.frame.join_inner(self.frame, "idnum")
        pd_joined_sparktk = joined_frame.to_pandas(joined_frame.count())
        pd_df = self.frame.to_pandas(self.frame.count())
        joined_pd = pd_df.merge(
            pd_df, on='idnum', suffixes=('_L', '_R'), how='inner')
        del pd_joined_sparktk['idnum']
        del joined_pd['idnum']
        self.assertItemsEqual(
            joined_pd.values.tolist(), pd_joined_sparktk.values.tolist())

    def test_empty_partner_left(self):
        """Join with empty frame Left"""
        join_frame = self.empty_frame.join_outer(self.frame, "idnum")
        self.assertEquals(6, join_frame.count())

    def test_empty_partner_right(self):
        """Join with empty frame Right"""
        join_frame = self.frame.join_outer(self.empty_frame, "idnum")
        self.assertEquals(6, join_frame.count())

    def test_empty_partner_inner(self):
        """Join with empty frame Inner"""
        join_frame = self.empty_frame.join_inner(
            self.empty_frame, "idnum")
        self.assertEquals(0, join_frame.count())

    def test_disjoint_outer(self):
        """Test with no overlaps in the join column, outer join"""
        join_frame = self.frame.join_outer(self.right_frame, "idnum")
        self.assertEquals(11, join_frame.count())

    def test_disjoint_left(self):
        """Test with no overlaps in the join column, left join"""
        join_frame = self.frame.join_left(self.right_frame, "idnum")
        self.assertEquals(6, join_frame.count())

    def test_disjoint_right(self):
        """Test with no overlaps in the join column, right join"""
        join_frame = self.frame.join_right(self.right_frame, "idnum")
        self.assertEquals(5, join_frame.count())

    def test_disjoint_inner(self):
        """Test with no overlaps in the join column, inner join"""
        join_frame = self.frame.join_inner(self.right_frame, "idnum")
        self.assertEquals(0, join_frame.count())

    def test_type_compatible(self):
        """Check compatibility among the numeric types"""
        block_data = [
            [0, "sifuri"],
            [1, "moja"],
            [2, "mbili"],
            [3, "tatu"],
            [4, "nne"],
            [5, "tano"]
        ]

        def complete_data_left(row):
            return block_data[int(row.idnum)]

        # Create a frame indexed by each of the numeric types
        int32_frame = self.context.frame.create(
            block_data, [("idnum", int), ("count", str)])
        flt32_frame = self.context.frame.create(
            block_data, [("idnum", float), ("count", str)])


        # int and float are not compatible with each other.
        with(self.assertRaisesRegexp(
                Exception, "Join columns must have compatible data types")):
            flt32_frame.join_right(int32_frame, "idnum")
        with(self.assertRaisesRegexp(
                Exception, "Join columns must have compatible data types")):
            flt32_frame.join_right(int32_frame, "idnum")

    def test_type_fail(self):
        """test join fails on mismatched type"""
        bad_schema = [("a", str), ('idnum', str)]
        frame2 = self.context.frame.create(data=[], schema=bad_schema)

        with(self.assertRaisesRegexp(
                Exception,
                "Join columns must have compatible data types")):
            self.frame.join_inner(frame2, "idnum")

    def test_join_no_column_left(self):
        """ test join faults on invalid left column"""
        with(self.assertRaisesRegexp(
                Exception,
                "No column named no_such_column")):
            self.frame.join_inner(
                self.frame, left_on="no_such_column", right_on="idnum")

    def test_join_no_column_right(self):
        """ test join faults on ivalid right column"""
        with(self.assertRaisesRegexp(
                Exception,
                "No column named no_such_column")):
            self.frame.join_inner(
                self.frame, left_on="idnum", right_on="no_such_column")

    def test_join_no_column_either(self):
        """ test join faults on invalid left and right column"""
        with(self.assertRaisesRegexp(Exception, "No column named no_column")):
            self.frame.join_inner(
                self.frame, left_on="no_column", right_on="no_column")

    def test_join_empty_column(self):
        """ test join faults with empty string for column"""
        with(self.assertRaisesRegexp(Exception, "No column named")):
            self.frame.join_inner(self.frame, left_on="idnum", right_on="")


if __name__ == "__main__":
    unittest.main()
