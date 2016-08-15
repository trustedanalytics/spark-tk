""" Tests unflatten functionality"""

import unittest
from sparktkregtests.lib import sparktk_test


class Unflatten(sparktk_test.SparkTKTestCase):

    def test_unflatten_one_column(self):
        """ test for unflatten comma-separated rows """
        datafile_unflatten = self.get_file("unflatten_data_no_spaces.csv")
        schema_unflatten = [("user", str),
                            ("day", str),
                            ("time", str),
                            ("reading", int)]

        frame = self.context.frame.import_csv(datafile_unflatten, schema=schema_unflatten)
        self.name_lookup = {}

        for row in frame.take(frame.row_count)[0]:
            if row[0] not in self.name_lookup:
                self.name_lookup[row[0]] = row
            else:
                row_copy = self.name_lookup[row[0]]
                for index in range(1, len(row_copy)):
                    row_copy[index] = str(row_copy[index]) + "," + str(row[index])
                self.name_lookup[row[0]] = row_copy

        frame.unflatten_columns(['user']) # also test with multi-column include "day"
        
        for row in frame.take(frame.row_count)[0]:
            self.assertEqual(self.name_lookup[row[0]], row)
        self.assertEqual(frame.row_count, 5)
    
    def test_unflatten_sparse_data(self):
        datafile_unflatten_sparse = self.get_file("unflatten_data_sparse.csv")
        schema_unflatten_sparse = [("user", int),
                                   ("day", str),
                                   ("time", str),
                                   ("reading", str)]

        frame_sparse = self.context.frame.import_csv(
            datafile_unflatten_sparse, schema=schema_unflatten_sparse) 
        name_lookup_sparse = {}
        for row in frame_sparse.take(frame_sparse.row_count)[0]:
            if row[0] not in name_lookup_sparse:
                name_lookup_sparse[row[0]] = row
            else:
                row_copy = name_lookup_sparse[row[0]]
                for index in range(1, len(row_copy)):
                    row_copy[index] = str(row_copy[index]) + "," + str(row[index])
                name_lookup_sparse[row[0]] = row_copy
        
        frame_sparse.unflatten_columns(['user']) # multi column day
        unflat_sparse_copy = frame_sparse.download()
         
        for row in frame_sparse.take(frame_sparse.row_count)[0]:
            self.assertEqual(name_lookup_sparse[row[0]], row)


if __name__ == "__main__":
    unittest.main()
