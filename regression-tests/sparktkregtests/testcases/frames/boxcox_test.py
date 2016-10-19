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

""" Test frame.box_cox() and frame.reverse_box_cox()"""

import unittest
from sparktkregtests.lib import sparktk_test


class BoxCoxTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(BoxCoxTest, self).setUp()

        dataset =\
            [[5.8813080107727425], [8.9771372790941797], [8.9153072947470804],
            [8.1583747730768401], [0.35889585616853292]]
        schema = [("y", float)]

        self.frame = self.context.frame.create(dataset, schema=schema)

    def test_wt_default(self):
        """ Test behaviour for default params, lambda = 0 """
        self.frame.box_cox("y")

        actual = self.frame.to_pandas()["y_lambda_0.0"].tolist()
        expected =\
            [1.7717791879837133, 2.1946810429706676,
            2.1877697201262163, 2.0990449791729704, -1.0247230268174008]
        self.assertItemsEqual(actual, expected)

    def test_lambda(self):
        """ Test wt for lambda = 0.3 """
        self.frame.box_cox("y", 0.3)
        
        actual = self.frame.to_pandas()["y_lambda_0.3"].tolist()
        expected =\
            [2.3384668540844573, 3.1056915770236082,
            3.0923547540771801, 2.9235756971904037, -0.88218677941017198]
        self.assertItemsEqual(actual, expected)

    def test_reverse_default(self):
        """ Test reverse transform for default lambda = 0 """
        self.frame.box_cox("y")
        self.frame.reverse_box_cox("y_lambda_0.0",
            reverse_box_cox_column_name="reverse")

        actual = self.frame.to_pandas()["reverse"].tolist()
        expected =\
            [5.8813080107727425, 8.9771372790941815,
            8.9153072947470804, 8.1583747730768401, 0.35889585616853298]

        self.assertItemsEqual(actual, expected)

    def test_reverse_lambda(self):
        """ Test reverse transform for lambda = 0.3 """
        self.frame.box_cox("y", 0.3)
        self.frame.reverse_box_cox("y_lambda_0.3", 0.3,
            reverse_box_cox_column_name="reverse")

        actual = self.frame.to_pandas()["reverse"].tolist()
        expected =\
            [5.8813080107727442, 8.9771372790941797,
            8.9153072947470822, 8.1583747730768419,
            0.35889585616853298]

        self.assertItemsEqual(actual, expected)

    @unittest.skip("req not clear")
    def test_lambda_negative(self):
        """ Test box cox for lambda -1 """
        self.frame.box_cox("y", -1)

        actual = self.frame.to_pandas()["y_lambda_-1.0"].tolist()
        expected =\
            [0.82996979614597488, 0.88860591423406388,
            0.88783336715839256, 0.87742656744575354,
            -1.7863236167608822]

        self.assertItemsEqual(actual, expected)

    def test_existing_boxcox_column(self):
        """ Test behavior for existing boxcox column """
        self.frame.box_cox("y", 0.3)
        
        with self.assertRaisesRegexp(
                Exception, "duplicate column name"):
            self.frame.box_cox("y", 0.3)

    def test_existing_reverse_column(self):
        """ Test behavior for existing reverse boxcox column """
        self.frame.reverse_box_cox("y", 0.3)

        with self.assertRaisesRegexp(
                Exception, "duplicate column name"):
            self.frame.reverse_box_cox("y", 0.3)

    @unittest.skip("Req not clear")
    def test_negative_col_positive_lambda(self):
        """Test behaviour for negative input column and positive lambda"""
        frame = self.context.frame.create([[-1], [-2], [1]], [("y", float)])
        frame.box_cox("y", 1)

        actual = frame.to_pandas()["y_lambda_1.0"].tolist()
        expected = [-2.0, -3.0, 0]

        self.assertItemsEqual(actual, expected)

    @unittest.skip("Req not clear")
    def test_negative_col_frational_lambda(self):
        """Test behaviour for negative input column and negative lambda"""
        frame = self.context.frame.create([[-1], [-2], [1]], [("y", float)])

        with self.assertRaises(Exception):
            frame.box_cox("y", 0.1)

    @unittest.skip("Req not clear")
    def test_negative_col_zero_lambda(self):
        """Test behaviour for negative input column and positive lambda"""
        frame = self.context.frame.create([[-1], [-2], [1]], [("y", float)])

        with self.assertRaises(Exception):
            frame.box_cox("y")

if __name__ == "__main__":
    unittest.main()
