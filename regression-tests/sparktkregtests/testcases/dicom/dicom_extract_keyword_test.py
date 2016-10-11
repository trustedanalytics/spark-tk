"""tests dicom.extract_keywords functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import numpy
from lxml import etree


class DicomExtractKeywordsTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(DicomExtractKeywordsTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = "../../../datasets/dicom/dicom_uncompressed/xml/"
        self.image_directory = "../../../datasets/dicom/dicom_uncompressed/imagedata/"
        self.count = self.dicom.metadata.count()

    def test_extract_one_column_basic(self):
        """test extract keyword with one col"""
        self.dicom.extract_keywords(["PatientID"])

        # ensure column was added
        columns = self.dicom.metadata.column_names
        if u'PatientID' not in columns:
            raise Exception("PatientID not added to columns")

        # compare expected results with extract_keywords result
        expected_result = self._get_expected_column_data_from_xml(["PatientID"])
        take_result = self.dicom.metadata.take(self.count, columns=['PatientID'])
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_multiple_columns_basic(self):
        """test extract keywords with mult cols"""
        keywords = ["PatientID", "SOPInstanceUID"]
        self.dicom.extract_keywords(keywords)

        # ensure columns were added
        columns = self.dicom.metadata.column_names
        if u'PatientID' not in columns:
            raise Exception("PatientID not added to columns")
        if u'SOPInstanceUID' not in columns:
            raise Exception("SOPInstanceUID not added to columns")

        # compare expected and actual result
        expected_result = self._get_expected_column_data_from_xml(keywords)
        take_result = self.dicom.metadata.take(self.count, columns=keywords)
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_invalid_column(self):
        """test extract keyword with invalid column"""
        self.dicom.extract_keywords(["invalid"])

        # ensure column was added
        columns = self.dicom.metadata.column_names
        if u'invalid' not in columns:
            raise Exception("Invalid column not added")

        # compare expected and actual result
        invalid_column = self.dicom.metadata.take(self.count, columns=[u'invalid'])
        expected_result = [[None] for x in range(0, self.count)]
        self.assertEqual(invalid_column, expected_result)

    def test_extract_multiple_invalid_columns(self):
        """test extract keyword mult invalid cols"""
        keywords = ["invalid", "another_invalid_col"]
        self.dicom.extract_keywords(keywords)

        # test that columns were added
        columns = self.dicom.metadata.column_names
        if u'invalid' not in columns:
            raise Exception("invalid column not added to columns")
        if u'another_invalid_col' not in columns:
            raise Exception("another_invalid_col not added to columns")

        # compare actual with expected result
        invalid_columns = self.dicom.metadata.take(self.count, columns=keywords)
        expected_result = [[None, None] for x in range(0, self.count)]
        self.assertEqual(invalid_columns, expected_result)

    def test_extract_invalid_valid_col_mix(self):
        keywords = ["PatientID", "Invalid"]
        self.dicom.extract_keywords(keywords)

        # test that columns were added
        columns = self.dicom.metadata.column_names
        if u'PatientID' not in columns:
            raise Exception("PatientID not added to columns")
        if u'Invalid' not in columns:
            raise Exception("Invalid not added to columns")

        # compare actual with expected result
        take_result = self.dicom.metadata.take(self.count, columns=keywords)
        expected_result = self._get_expected_column_data_from_xml(keywords)
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_invalid_type(self):
        with self.assertRaisesRegexp(Exception, "should be either str or list"):
            self.dicom.extract_keywords(1)

    def test_extract_unicode_columns(self):
        keywords = [u'PatientID']
        self.dicom.extract_keywords(keywords)

        # test that column was added
        columns = self.dicom.metadata.column_names
        if u'PatientID' not in columns:
            raise Exception("PatientID not added to columns")

        # compare actual with expected result
        take_result = self.dicom.metadata.take(self.count, columns=keywords)
        expected_result = self._get_expected_column_data_from_xml(keywords)
        numpy.testing.assert_equal(take_result, expected_result)

    def _get_expected_column_data_from_xml(self, tags):
        # generate expected data by extracting the keywords ourselves
        expected_column_data = []

        # download to pandas for easy access
        metadata_pandas = self.dicom.metadata.to_pandas()

        # iterate through the metadata rows
        for index, row in metadata_pandas.iterrows():
            # convert metadata to ascii string
            metadata = row["metadata"].encode("ascii", "ignore")
            # create a lxml tree object from xml metadata
            xml_root = etree.fromstring(metadata)

            expected_row = []
            for tag in tags:
                # for lxml the search query means
                # look for all DicomAttribute elements with
                # attribute keyword equal to our keyword
                # then get the value element underneath it and extract the
                # inner text
                tag_query = ".//DicomAttribute[@keyword='" + tag + "']/Value/text()"
                query_result = xml_root.xpath(tag_query)

                # if result is [] use None, otherwise format in unicode
                result = query_result[0].decode("ascii", "ignore") if query_result else None
                expected_row.append(result)

                #expected_row.append(query_result)
            expected_column_data.append(expected_row)
        return expected_column_data


if __name__ == "__main__":
    unittest.main()
