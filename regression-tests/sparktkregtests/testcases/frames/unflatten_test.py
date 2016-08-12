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

        datafile_unflatten_sparse = self.get_file("unflatten_data_sparse.csv")
        schema_unflatten_sparse = [("user", int),
                                   ("day", str),
                                   ("time", str),
                                   ("reading", str)]

        frame_sparse = self.context.frame.import_csv(
            datafile_unflatten_sparse, schema=schema_unflatten_sparse)
        
        frame_copy = frame.download()
        frame.unflatten_columns(['user', 'day'])

        print "frame_copy: " + str(frame_copy.take(frame_copy.row_count))


        unflat_copy = frame.download()
        print "unflat_copy: " + str(unflat_copy.take(unflat_copy.row_count))
        for (unflatrow, row) in zip(unflat_copy.sort(['user', 'day', 'time', 'reading']).iterrows(), frame_copy.sort(['user', 'day', 'time', 'reading']).iterrows()):
            print "unflatten copy row time: " + str(unflatrow)
            print "frame row time: " + str(row)
            self.assertEqual(
                len(str(unflatrow['time']).split(',')),
                len(str(unflatrow['reading']).split(',')))

        self.assertEqual(frame.row_count, 5)

        frame_sparse.unflatten_columns(['user', 'day'])
        unflat_sparse_copy = frame_sparse.download()

        for index, row in unflat_sparse_copy.iterrows():
            self.assertEqual(
                len(str(row['time']).split(',')),
                len(str(row['reading']).split(',')))

        self.assertEqual(frame_sparse.row_count, 2 * 100)


if __name__ == "__main__":
    unittest.main()
