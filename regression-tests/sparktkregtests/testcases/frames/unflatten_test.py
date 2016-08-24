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
        # download as a pandas frame to access data
        pandas_frame = frame.download()
        pandas_data = []
        # use this dictionary to store expected results by name
        name_lookup = {}

        # generate our expected results by unflattening
        # each row ourselves and storing it by name
        for index, row in pandas_frame.iterrows():
            # if the name is not already in name lookup
            # index 0 refers to user name (see schema above)
            if row['user'] not in name_lookup:
                name_lookup[row['user']] = row
            else:
                row_copy = name_lookup[row['user']]
                # append each item in the row to a comma
                # delineated string
                for index in range(1, len(row_copy)):
                    row_copy[index] = str(row_copy[index]) + "," + str(row[index])
                name_lookup[row['user']] = row_copy

        # now we unflatten the columns using sparktk
        frame.unflatten_columns(['user'])
        
        # finally we iterate through what we got
        # from sparktk unflatten and compare
        # it to the expected results we created
        unflatten_pandas = frame.download()
        for index, row in unflatten_pandas.iterrows():
            self.assertTrue(row.equals(name_lookup[row['user']]))
        self.assertEqual(frame.count(), 5)

    def test_unflatten_multiple_cols(self):
        frame = self.context.frame.import_csv(self.datafile_unflatten,
                schema=self.schema_unflatten)
        # download a pandas frame of the data
        pandas_frame = frame.download()
        name_lookup = {}

        # same logic as for unflatten_one_column,
        # generate our expected results by unflattening
        for index, row in pandas_frame.iterrows():
            if row['user'] not in name_lookup:
                name_lookup[row['user']] = row
            else:
                row_copy = name_lookup[row['user']]
                for index in range(1, len(row_copy)):
                    # then only difference between multi col and single col
                    # is that we will expect any data in the column at index 1
                    # (which is day, see the schema above)
                    # to also be flattened
                    # so we only append data if it isn't already there
                    if index is not 1 or str(row[index]) not in str(row_copy[index]):
                        row_copy[index] = str(row_copy[index]) + "," + str(row[index])
                name_lookup[row['user']] = row_copy

        # now we unflatten using sparktk
        frame.unflatten_columns(['user', 'day'])

        # and compare our expected data with sparktk results
        # which we have downloaded as a pandas frame
        unflatten_pandas = frame.download()
        for index, row in unflatten_pandas.iterrows():
            self.assertTrue(row.equals(name_lookup[row['user']]))

    # same logic as single column but with sparse data
    # because there are so many rows in this data
    # (datafile contains thousands and thousands of lines)
    # we will not compare the content of the unflatten frame
    # as this would require us to iterate multiple times
    def test_unflatten_sparse_data(self):
        datafile_unflatten_sparse = self.get_file("unflatten_data_sparse.csv")
        schema_unflatten_sparse = [("user", int),
                                   ("day", str),
                                   ("time", str),
                                   ("reading", str)]
        frame_sparse = self.context.frame.import_csv(
            datafile_unflatten_sparse, schema=schema_unflatten_sparse)

        frame_sparse.unflatten_columns(['user'])
        pandas_frame_sparse = frame_sparse.download()

        # since this data set is huge we will just iterate once
        # to make sure the data has been appended for time and
        # reading for each row and that there are the correct
        # number of items that we would expect for the
        # unflattened frame
        for index, row in pandas_frame_sparse.iterrows():
            self.assertEqual(
                    len(str(row['time']).split(",")),
                    len(str(row['reading']).split(",")))
        self.assertEqual(frame_sparse.count(), 100)
        

if __name__ == "__main__":
    unittest.main()
