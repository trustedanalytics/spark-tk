"""Tests methods that access or alter columns"""
import unittest
import exceptions as e

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from qalib import sparktk_test

dummy_int_val = -77     # placeholder data value for added column
dummy_col_count = 1000  # length of dummy list for column add


# This method is to test different sources of functions
# i.e. global
def global_dummy_val_list(row):
    return [dummy_int_val for _ in range(0, dummy_col_count)]


class ColumnMethodTest(sparktk_test.SparkTKTestCase):

    # Test class bound methods
    @staticmethod
    def static_dummy_val_list(row):
        return [dummy_int_val for _ in range(0, dummy_col_count)]

    def setUp(self):
        """Build test_frame"""
        super(ColumnMethodTest, self).setUp()
        dataset = self.get_file("int_str_float.csv")
        schema = [("int", int), ("str", str), ("float", float)]

        self.frame = self.context.frame.import_csv(dataset, schema=schema)

    def test_rename_columns(self):
        """Test renaming columns works"""
        self.frame.add_columns(
            lambda row: dummy_int_val, ('product', int))

        col_count = len(self.frame.take(1).data[0])
        self.frame.rename_columns(
            {'int': 'firstNumber', 'float': 'secondNumber'})

        self.assertEqual(col_count, len(self.frame.take(1).data[0]))
        self.assertNotIn('int', self.frame.column_names)
        self.assertNotIn('float', self.frame.column_names)
        self.assertIn('firstNumber', self.frame.column_names)
        self.assertIn('secondNumber', self.frame.column_names)

    def test_redundant_rename(self):
        """Test renaming with the same name works"""
        col_count = len(self.frame.take(1).data[0])
        self.frame.rename_columns({'str': 'str'})
        self.assertEqual(col_count, len(self.frame.take(1).data[0]))
        self.assertIn('str', self.frame.column_names)

    def test_swap_column_names(self):
        """Test swapping column names works"""
        col_count = len(self.frame.take(1).data[0])
        self.frame.rename_columns({'str': 'int', 'int': 'str'})
        self.assertEqual(col_count, len(self.frame.take(1).data[0]))
        self.assertEqual(u'str', self.frame.column_names[0])
        self.assertEqual(u'int', self.frame.column_names[1])

    @unittest.skip("column name collisions not honored")
    def test_multi_column_names_collision_no_force(self):
        """Test a multiple column rename with collisions fails"""
        with self.assertRaises(ValueError):
            self.frame.rename_columns({'str': 'alpha',
                                       'int': 'times',
                                       'float': 'alpha'})

    @unittest.skip("column name collisions not honored")
    def test_rename_existing_name_no_force(self):
        """Test renaming to an existing name errors"""
        with self.assertRaises(ValueError):
            self.frame.rename_columns({'int': 'str'})

    @unittest.skip("column name collisions not honored")
    def test_multi_column_names_collision(self):
        """Test a multiple column rename with collisions fails"""
        with self.assertRaises(ValueError):
            self.frame.rename_columns({'str': 'alpha',
                                       'int': 'times',
                                       'float': 'alpha'})
            self.frame.inspect()

    @unittest.skip("column name collisions not honored")
    def test_rename_existing_name(self):
        """Test renaming to an existing name errors"""
        with self.assertRaises(ValueError):
            self.frame.rename_columns({'int': 'str'})
            self.frame.inspect()

    def test_rename_non_existent(self):
        """Test renaming a non-existent column fails"""
        with self.assertRaisesRegexp(Exception, "Invalid column"):
            self.frame.rename_columns({'no-such-name': 'not-a-name'})

    def test_rename_with_special_characters(self):
        """Test renaming with special characters errors"""
        with self.assertRaisesRegexp(
            Exception, "alpha-numeric"):
            self.frame.rename_columns(
                {'int': 'Long ugly !@#$%^&*(?)_+|}{[\\]\|'})


if __name__ == "__main__":
    unittest.main()
