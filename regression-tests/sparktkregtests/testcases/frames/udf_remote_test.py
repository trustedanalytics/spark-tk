""" Test UDF implementation."""
import unittest
from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib.udftestlib import udf_remote_utils_direct, udf_remote_utils_indirect, udf_remote_utils_select


class UDFTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        super(UDFTest, self).setUp()
        data_basic = self.get_file("int_str_int.csv")
        schema_basic = [("num1", int),
                        ("letter", str),
                        ("num2", int)]
        self.frame = self.context.frame.import_csv(data_basic,
                                                   schema=schema_basic)

    def test_udf_basic_module_install(self):
        """First test case from UDF testing"""
        # add columns using udf utils
        self.frame.add_columns(
            lambda row:
            udf_remote_utils_direct.length(row.letter) + row.num2,
            ('new_column', int))
        self.frame.add_columns(
            udf_remote_utils_direct.row_build, ('other_column', int))

        # get frame data for the entire frame and for the middle col
        frame_take = self.frame.take(self.frame.row_count)
        letter_col_take = self.frame.take(self.frame.row_count,
                                          columns=['letter'])

        # extract just the letter column into an array of strings 
        letter = [x[0].encode("ascii", "ignore") for x in letter_col_take.data]
        
        # get the index for the row containing a and c and verify
        a_row = letter.index('a')
        c_row = letter.index('c')
        self.assertEqual(frame_take.data[c_row][-1], len(frame_take.data[c_row][1]))
        self.assertEqual(frame_take.data[a_row][3], frame_take.data[a_row][2]+1)

    def test_udf_indirect_std(self):
        """test indirect udf usage"""
        # add columns using udf utils
        self.frame.add_columns(
            udf_remote_utils_indirect.distance, ('other_column', float))

        # get the data for the entire frame and just the letter col
        frame_take = self.frame.take(self.frame.row_count)
        letter_col_take = self.frame.take(self.frame.row_count,
                                          columns=['letter'])

        # extract just the letter column into an array of strs
        letter = [x[0].encode("ascii", "ignore") for x in letter_col_take.data]

        # get the index of the col containing a and c and verify
        a_row = letter.index('a')
        c_row = letter.index('c')
        self.assertAlmostEqual(
            frame_take.data[c_row][-1], frame_take.data[c_row][0] * 2.2360679775)
        self.assertAlmostEqual(
            frame_take.data[a_row][-1], frame_take.data[a_row][0] * 2.2360679775)

    def test_udf_indirect_delayed(self):
        """Call a remote function."""
        # logic is similar to above two tests
        udf_remote_utils_select.add_select_col(self.frame)

        frame_take = self.frame.take(self.frame.row_count)
        letter_col_take = self.frame.take(self.frame.row_count, columns=['letter'])
        
        letter = [x[0].encode("ascii", "ignore") for x in letter_col_take.data]
        
        a_row = letter.index('a')
        b_row = letter.index('b')
        self.assertEqual(frame_take.data[a_row][-1], frame_take.data[a_row][0])
        self.assertEqual(frame_take.data[b_row][-1], frame_take.data[b_row][2])

    #@unittest.skip("add columns does not error with invalid func")
    def test_udf_indirect_missing(self):
        """Use a function that is missing a support function (indirect call)"""
        with self.assertRaisesRegexp(Exception, "An error occurred"):
            # git add columns an invalid function param
            self.frame.add_columns(
                lambda row: bogus_dependency.not_exist(row.letter) + row.num2,
                ('new_column', int))
            # because frames are created lazily we must perform
            # some kind of op on the frame to trigger the exception
            self.frame.row_count

if __name__ == "__main__":
    unittest.main()
