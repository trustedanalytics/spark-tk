""" Tests unflatten functionality"""

import unittest
from sparktkregtests.lib import sparktk_test


class Unflatten(sparktk_test.SparkTKTestCase):

    def setUp(self):
        super(Unflatten, self).setUp()
        self.datafile_unflatten = self.get_file("unflatten_data_no_spaces.csv")
        self.schema_unflatten = [("user", str),
                            ("day", str),
                            ("time", str),
                            ("reading", int)]

    def test_unflatten_one_column(self):
        """ test for unflatten comma-separated rows """
        frame = self.context.frame.import_csv(self.datafile_unflatten,
                schema=self.schema_unflatten)
        # use this dictionary to store expected results by name
        name_lookup = {}

        # generate our expected results by unflattening
        # each row ourselves and storing it by name
        for row in frame.take(frame.row_count)[0]:
            # if the name is not already in name lookup
            # index 0 refers to user name (see schema above)
            if row[0] not in name_lookup:
                name_lookup[row[0]] = row
            else:
                row_copy = name_lookup[row[0]]
                # append each item in the row to a comma
                # delineated string
                for index in range(1, len(row_copy)):
                    row_copy[index] = str(row_copy[index]) + "," + str(row[index])
                name_lookup[row[0]] = row_copy

        # now we unflatten the columns using sparktk
        frame.unflatten_columns(['user'])

        # finally we iterate through what we got
        # from sparktk unflatten and compare
        # it to the expected results we created
        for row in frame.take(frame.row_count)[0]:
            self.assertEqual(name_lookup[row[0]], row)
        self.assertEqual(frame.row_count, 5)

    def test_unflatten_multiple_cols(self):
        frame = self.context.frame.import_csv(self.datafile_unflatten,
                schema=self.schema_unflatten)
        name_lookup = {}

        # same logic as for unflatten_one_column,
        # generate our expected results by unflattening
        for row in frame.take(frame.row_count)[0]:
            if row[0] not in name_lookup:
                name_lookup[row[0]] = row
            else:
                row_copy = name_lookup[row[0]]
                for index in range(1, len(row_copy)):
                    # then only difference between multi col and single col
                    # is that we will expect any data in the column at index 1
                    # (which is day, see the schema above)
                    # to also be flattened
                    # so we only append data if it isn't already there
                    if index is not 1 or str(row[index]) not in str(row_copy[index]):
                        row_copy[index] = str(row_copy[index]) + "," + str(row[index])
                name_lookup[row[0]] = row_copy

        # now we unflatten using sparktk
        frame.unflatten_columns(['user', 'day'])

        # and compare our expected data with sparktk results
        for row in frame.take(frame.row_count)[0]:
            self.assertEqual(name_lookup[row[0]], row)

    # same logic as single column but with sparse data
    def test_unflatten_sparse_data(self):
        datafile_unflatten_sparse = self.get_file("unflatten_data_sparse.csv")
        schema_unflatten_sparse = [("user", int),
                                   ("day", str),
                                   ("time", str),
                                   ("reading", str)]
        frame_sparse = self.context.frame.import_csv(
            datafile_unflatten_sparse, schema=schema_unflatten_sparse)
        name_lookup_sparse = {}

        # similar logic to the above tests
        for row in frame_sparse.take(frame_sparse.row_count)[0]:
            if row[0] not in name_lookup_sparse:
                name_lookup_sparse[row[0]] = row
            else:
                row_copy = name_lookup_sparse[row[0]]
                for index in range(1, len(row_copy)):
                    row_copy[index] = str(row_copy[index]) + "," + str(row[index])
                name_lookup_sparse[row[0]] = row_copy

        frame_sparse.unflatten_columns(['user'])

        # compare sparktk results with expected data
        for row in frame_sparse.take(frame_sparse.row_count)[0]:
            self.assertEqual(name_lookup_sparse[row[0]], row)


if __name__ == "__main__":
    unittest.main()
