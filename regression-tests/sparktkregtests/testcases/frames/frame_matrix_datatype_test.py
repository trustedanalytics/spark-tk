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

""" Tests matrix datatype on frames """

import unittest
import numpy
from itertools import ifilter, imap
from sparktkregtests.lib import sparktk_test
from sparktk.dtypes import matrix, vector


class FrameMatrixDataTypeTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build frames to be exercised and establish known baselines"""
        super(FrameMatrixDataTypeTest, self).setUp()
        self.dataset = [["A", [[1,2],[3,4]]], ["B", [[5,6],[7,8]]], ["C", [[9,10],[11,12],[13,14]]]]
        self.schema = [("C0", str), ("C1", matrix)]
        self.frame = self.context.frame.create(self.dataset,
                                               schema=self.schema)

    def test_frame_create_row_count(self):
        """ Trivial Frame creation. """
        frame = self.context.frame.create(self.dataset,
                                          schema=self.schema)
        self.assertEqual(frame.count(), len(self.dataset))
        self.assertEqual(len(frame.take(3)), 3)
        # test to see if taking more rows than exist still
        # returns only the right number of rows
        self.assertEqual(len(frame.take(10)), len(self.dataset))

    @unittest.skip("sparktk: schema inference between matrix and vector is ambiguous")
    def test_without_schema(self):
        """Test without a specified schema"""
        frame = self.context.frame.create(self.dataset)
        self.assertEqual(frame.schema, self.schema)

    @unittest.skip("sparktk: schema inference between matrix and vector is ambiguous")
    def test_with_validate_schema_no_schema_provided(self):
        """Test without a specified schema validating the schema"""
        frame = self.context.frame.create(self.dataset, validate_schema=True)
        self.assertEqual(frame.schema, self.schema)

    def test_with_validate_schema_with_valid_schema(self):
        """Test with validate_schema true and also a valid schema"""
        # should default to using the defined schema
        frame = self.context.frame.create(self.dataset,
                                          validate_schema=True,
                                          schema=self.schema)
        self.assertEqual(frame.schema, self.schema)

    def test_validate_schema_with_invalid_schema_all_columns_same_datatype(self):
        """Test with validate_schema=True and invalid schema, columns same type"""
        invalid_schema = [("col1", int), ("col2", int)]
        validated_frame = self.context.frame.create(self.dataset,
                                                    validate_schema=True,
                                                    schema=invalid_schema)
        for row in validated_frame.take(validated_frame.count()):
            for item in row:
                if type(item) is not int:
                    self.assertEqual(item, None)

    def test_validate_schema_of_strs(self):
        """Test validate schema true with schema of strs"""
        schema = [("C0", str), ("C1", str)]
        # should not throw an exception
        # if the datatype can be cast to the schema-specified
        # datatype validate schema should just cast it
        # since ints and floats can be cast to string
        # it should not error but should cast all of the data to strings
        frame = self.context.frame.create(self.dataset, schema=schema, validate_schema=True)
        for row in frame.take(frame.count()):
            # the data should all be cast to str by validate_schema=True
            for item in row:
                self.assertEqual(type(item), str)

    def test_add_columns(self):
        """Test add columns on matrix column data"""
        frame = self.context.frame.create(self.dataset, self.schema)
        
        # Add the number of rows of the matrix as a column named shape 
        frame.add_columns(lambda row: row["C1"].shape[0], ('shape', int))
        obtained_result = frame.take(10, columns='shape')
        expected_result = [[numpy.array(item[1]).shape[0]] for item in self.dataset]
        self.assertEqual(obtained_result, expected_result)

    def test_filter(self):
        """Test filter on matrix column data"""
        frame = self.context.frame.create(self.dataset, self.schema)
        
        # Get number of rows in each matrix from shape of the underlying ndarray
        frame.filter(lambda row: row["C1"].shape[0] == 2)
        obtained_result = frame.count()
        obtained_result_matrix = frame.take(10, columns='C1')

        # Get expected result by converting the actual dataset to ndarray and testing the same condition
        filtered_result_matrix = list(ifilter(lambda i: numpy.array(i[1]).shape[0] == 2, self.dataset))
        expected_result_matrix = list(imap(lambda row: [numpy.array(row[1])], filtered_result_matrix))
        expected_result = len(expected_result_matrix)

        self.assertEqual(obtained_result, expected_result)
        numpy.testing.assert_array_equal(obtained_result_matrix, expected_result_matrix)

    def test_convert_matrix_col_to_vector(self):
        """ Convert a matrix column to vector using add_columns"""
        frame = self.context.frame.create(self.dataset, self.schema)
        
        # Filter the rows which have more than 2 rows as the final vector construction can be for only 2 values
        # as vector needs the length to be defined
        frame.filter(lambda row: row["C1"].shape[0] == 2)
        
        # Add first column of each matrix as a new column with vector data type
        frame.add_columns(lambda row: row["C1"][:,0], ('first_column', vector(2)))
        obtained_result = frame.take(10, columns='first_column')

        # Convert the first 2 elements of the dataset to numpy array and get the fist column
        expected_result = [[numpy.array(item[1])[:,0]] for item in self.dataset[:2]]
        numpy.testing.assert_array_equal(obtained_result, expected_result) 
        
if __name__ == "__main__":
    unittest.main()

