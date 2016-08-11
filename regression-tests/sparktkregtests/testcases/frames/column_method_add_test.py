"""Tests methods that access or alter columns"""
import unittest

from sparktkregtests.lib import sparktk_test

udf_int_val = -77     # placeholder data value for added column
udf_col_count = 1000  # length of list for column add


def global_udf(row):
    """This method is to test different sources of functions with udf"""
    return [udf_int_val for _ in range(0, udf_col_count)]


class ColumnMethodTest(sparktk_test.SparkTKTestCase):

    # Test class bound methods
    @staticmethod
    def static_udf(row):
        """This method is to test different sources of functions with udf"""
        return [udf_int_val for _ in range(0, udf_col_count)]

    def setUp(self):
        """Build test_frame"""
        super(ColumnMethodTest, self).setUp()
        dataset = self.get_file("int_str_float.csv")
        schema = [("int", int), ("str", str), ("float", float)]

        self.frame = self.context.frame.import_csv(dataset, schema=schema)
        old_header = self.frame.column_names
        self.new_col_schema = [("col_" + str(n), int)
                               for n in range(0, udf_col_count)]
        self.expected_header = old_header + [col_schema[0]
                                             for col_schema in
                                             self.new_col_schema]

    def test_static_add_col_names(self):
        """Tests adding a column name with a static method"""
        self.frame.add_columns(
            ColumnMethodTest.static_udf, self.new_col_schema)
        self.assertEqual(self.frame.column_names, self.expected_header)
        self.assertEqual(
            len(self.new_col_schema)+3, len((self.frame.take(1)).data[0]))

        columns = self.frame.take(self.frame.count()).data
        for i in columns:
            self.assertEqual(i[-1], udf_int_val)

    @unittest.skip("Spark global udf doesn't autoamtically add script")
    def test_add_col_names(self):
        """Tests adding a column name with a global method"""
        self.frame.add_columns(global_udf, self.new_col_schema)
        self.assertEqual(self.frame.column_names, self.expected_header)

        self.assertEqual(
            len(self.new_col_schema)+3, len((self.frame.take(1)).data[0]))

        self.frame.inspect()
        columns = self.frame.take(self.frame.count()).data
        for i in columns:
            self.assertEqual(i[-1], udf_int_val)

    def test_add_columns_lambda_single(self):
        """Test adding individual columns from a lambda"""
        col_count = len((self.frame.take(1)).data[0])
        self.frame.add_columns(
            lambda row: row.int*row.float, ('a_times_b', int))
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1)).data[0]))

    def test_add_columns_lambda_multiple(self):
        """Test adding multiple columns from a lambda"""
        col_count = len((self.frame.take(1)).data[0])
        self.frame.add_columns(
            lambda row: [row.int * row.float, row.int + row.float],
            [("a_times_b", float), ("a_plus_b", float)])
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertIn('a_plus_b', self.frame.column_names)
        self.assertEqual(col_count+2, len((self.frame.take(1)).data[0]))

    def test_add_columns_accessed_str(self):
        """Test columns_accessed param, 1 column, string"""
        col_count = len((self.frame.take(1)).data[0])
        self.frame.add_columns(
            lambda row: row.int*row.int, ('a_times_b', int), 'int')
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1)).data[0]))

    def test_add_columns_accessed_list1(self):
        """Test columns_accessed param, 1 column, list"""
        col_count = len((self.frame.take(1)).data[0])
        self.frame.add_columns(
            lambda row: row.int*row.int, ('a_times_b', int), ['int'])
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1)).data[0]))

    def test_add_columns_accessed_extra(self):
        """Test columns_accessed param; specify an unused column."""
        col_count = len((self.frame.take(1)).data[0])
        self.frame.add_columns(
            lambda row: row.int*row.int,
            ('a_times_b', int), ['int', 'float'])
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1)).data[0]))

    def test_add_columns_accessed_list_all(self):
        """Test columns_accessed param, 1 column, list of all"""
        col_count = len((self.frame.take(1)).data[0])
        self.frame.add_columns(
            lambda row: float(ord(row.str[0])) / (row.float*row.int),
            ('a_times_b', float), ['int', 'str', 'float'])
        self.assertIn('a_times_b', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1)).data[0]))

    def test_add_columns_accessed_list_ooo(self):
        """Test columns_accessed param, all columns out of order"""
        col_count = len((self.frame.take(1)).data[0])
        self.frame.add_columns(
            lambda row: float(ord(row.str[0])) / (row.int*row.float),
            ('poutine', float), ['float', 'int', 'str'])
        self.assertIn('poutine', self.frame.column_names)
        self.assertEqual(col_count+1, len((self.frame.take(1)).data[0]))

    @unittest.skip("accessed columns is not honored")
    def test_add_columns_accessed_miss(self):
        """Test columns_accessed param, 1 of 2 needed columns"""
        with self.assertRaises(ValueError):
            self.frame.add_columns(
                lambda row: row.int*row.float,
                ('a_times_b', int), ['int'])
            self.frame.inspect()

    @unittest.skip("accessed columns is not honored")
    def test_add_columns_accessed_nosuch(self):
        """Test columns_accessed param, non-existent col."""
        with self.assertRaises(ValueError):
            self.frame.add_columns(
                lambda row: row.int*row.int,
                ('b_times_b', int), ['no_such_col'])
            self.frame.inspect()

    def test_add_columns_abort(self):
        """Test divide by zero errors"""
        # Divide by 0 exception will abort column add;
        # Schema should be unchanged.
        schema_before = self.frame.schema

        def bad_divide(row):
            return float(row.float) / 0

        with self.assertRaisesRegexp(ValueError, "Unsupported type e"):
            self.frame.add_columns(
                bad_divide, schema=["result", float])
            self.assertEqual(schema_before, self.frame.schema)
            self.frame.inspect()
        self.assertEqual(schema_before, self.frame.schema)

    @unittest.skip("column names not validated")
    def test_add_columns_add_existing_name(self):
        """Test adding columns with existing names errors"""
        with self.assertRaises(ValueError):
            self.frame.add_columns(lambda row: udf_int_val, ('str', int))
            self.frame.inspect()

    @unittest.skip("column names not validated")
    def test_add_column_with_empty_name(self):
        """Test adding a column with an empty name errors"""
        with self.assertRaises(ValueError):
            self.frame.add_columns(lambda row: udf_int_val, ('', int))
            self.frame.inspect()

    @unittest.skip("column names not validated")
    def test_add_column_null_schema_no_force(self):
        """Test adding a column with a null schema errors, don't force eval"""
        with self.assertRaises(TypeError):
            self.frame.add_columns(lambda row: udf_int_val, None)

    @unittest.skip("column names not validated")
    def test_add_column_empty_schema_no_force(self):
        """Test adding a column with empty schema errors, don't force eval"""
        with self.assertRaises(ValueError):
            self.frame.add_columns(lambda row: udf_int_val, ())

    def test_add_column_null_schema(self):
        """Test adding a column with a null schema errors"""
        with self.assertRaisesRegexp(
                TypeError, "'NoneType' object has no attribute '__getitem__'"):
            self.frame.add_columns(lambda row: udf_int_val, None)
            self.frame.inspect()

    def test_add_column_empty_schema(self):
        """Test adding a column with an empty schema errors"""
        with self.assertRaisesRegexp(IndexError, "tuple index out of range"):
            self.frame.add_columns(lambda row: udf_int_val, ())
            self.frame.inspect()

    def test_add_column_schema_list(self):
        """Test adding a column with a schema containing a list"""
        with self.assertRaisesRegexp(Exception, 'concatenate list'):
            self.frame.add_columns(
                    lambda row: udf_int_val, schema=[('new_col', int)])
            self.frame.inspect()

    def test_unicode_conversion(self):
        """Test renaming with unicode names"""
        self.frame.add_columns(
            lambda row: udf_int_val, ('product', int))
        col_count = len(self.frame.take(1).data[0])
        self.frame.rename_columns({'product': u'unicode'})
        self.assertEqual(col_count, len(self.frame.take(1)[0][0]))
        self.assertNotIn('product', self.frame.column_names)
        self.assertIn(u'unicode', self.frame.column_names)


if __name__ == "__main__":
    unittest.main()
