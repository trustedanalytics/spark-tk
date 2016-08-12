""" Tests unflatten functionality"""

import unittest
from sparktkregtests.lib import sparktk_test


class Unflatten(sparktk_test.SparkTKTestCase):

    def test_unflatten_basic(self):
        """ test for unflatten comma-separated rows """
        datafile_unflatten = self.get_file("unflatten_data_no_spaces.csv")
        schema_unflatten = [("user", str),
                            ("day", str),
                            ("time", str),
                            ("reading", int)]

        frame = self.context.frame.import_csv(datafile_unflatten, schema=schema_unflatten)
        frame_copy = frame.copy()
        print ""
        print ""
        print "frame copy: " + str(frame_copy.inspect())
        print ""


        datafile_unflatten_sparse = self.get_file("unflatten_data_sparse.csv")
        schema_unflatten_sparse = [("user", int),
                                   ("day", str),
                                   ("time", str),
                                   ("reading", str)]

        frame_sparse = self.context.frame.import_csv(
            datafile_unflatten_sparse, schema=schema_unflatten_sparse)
        
        frame_copy_pandas = frame.download()
        frame.unflatten_columns(['user', 'day'])
        unflat_copy = frame
        
        print "unflat_copy: " + str(unflat_copy.inspect())
        print ""
        print "frame copy again: " + str(frame_copy.inspect())
        for (unflatrow, row) in zip(unflat_copy.take(unflat_copy.row_count), frame_copy.take(frame_copy.row_count)):
            self.assertEqual(unflatrow, row)

        self.assertEqual(frame.row_count, 5)

        frame_sparse.unflatten_columns(['user', 'day'])
        unflat_sparse_copy = frame_sparse.download()


        #for row, unflatRow in zip(frame_copy_pandas.iterrows(), unflat_sparse_copy.iterrows()):
            #print "pandas frame row: " + str(row)
            #print "pandas frame unflatten row: " + str(unflatRow)
            #print "pandas frame row time split: " + str(str(row['time']).split(','))
            #print "pandas frame reading split: " + str(str(row['reading']).split(','))


        for index, row in unflat_sparse_copy.iterrows():
            #print "row time split: " + str(str(row['time']).split(','))
            #print "row reading split: " + str(str(row['reading']).split(','))
            #print "pandas frame unflattened row: " + str(row)
            self.assertEqual(
                len(str(row['time']).split(',')),
                len(str(row['reading']).split(',')))

        self.assertEqual(frame_sparse.row_count, 2 * 100)


if __name__ == "__main__":
    unittest.main()
