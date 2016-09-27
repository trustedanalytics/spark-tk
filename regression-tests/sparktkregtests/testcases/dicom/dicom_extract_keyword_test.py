"""tests dicom.filter functionality"""

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
        self.dicom.extract_keywords(["PatientID"])
        columns = self.dicom.metadata.column_names
        if u'PatientID' not in columns:
            raise Exception("PatientID not added to columns")
        expected_result = self._get_expected_column_data_from_xml(["PatientID"])
        take_result = self.dicom.metadata.take(self.count, columns=[u'PatientID']).data
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_multiple_columns_basic(self):
        self.dicom.extract_keywords(["PatientID", "SOPInstanceUID"])
        columns = self.dicom.metadata.column_names
        if u'PatientID' not in columns:
            raise Exception("PatientID not added to columns")
        if u'SOPInstanceUID' not in columns:
            raise Exception("SOPInstanceUID not added to columns")

    def test_extract_invalid_column(self):
        self.dicom.extract_keywords(["invalid"])
        columns = self.dicom.metadata.column_names
        if u'invalid' not in columns:
            raise Exception("Invalid column not added")
        invalid_column = self.dicom.metadata.take(self.count, columns=[u'invalid']).data
        expected_result = [[None] for x in range(0, self.count)]
        self.assertEqual(invalid_column, expected_result)

    def test_extract_multiple_invalid_columns(self):
        self.dicom.extract_keywords(["invalid", "another_invalid_col"])
        columns = self.dicom.metadata.column_names
        if u'invalid' not in columns:
            raise Exception("invalid column not added to columns")
        if u'another_invalid_col' not in columns:
            raise Exception("another_invalid_col not added to columns")
        invalid_columns = self.dicom.metadata.take(self.count, columns=[u'invalid', u'another_invalid_col']).data
        expected_result = [[None, None] for x in range(0, self.count)]
        self.assertEqual(invalid_columns, expected_result)

    def test_extract_invalid_valid_col_mix(self):
        self.dicom.extract_keywords(["PatientID", "Invalid"])
        columns = self.dicom.metadata.column_names
        if u'PatientID' not in columns:
            raise Exception("PatientID not added to columns")
        if u'Invalid' not in columns:
            raise Exception("Invalid not added to columns")
        take_result = self.dicom.metadata.take(self.count, columns=[u'PatientID', u'Invalid']).data
        expected_result = self._get_expected_column_data_from_xml(["PatientID", "Invalid"])
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_invalid_type(self):
        with self.assertRaisesRegexp(Exception, "should be either str or list"):
            self.dicom.extract_keywords(1)

    def test_extract_unicode_columns(self):
        self.dicom.extract_keywords([u'PatientID'])
        columns = self.dicom.metadata.column_names
        if u'PatientID' not in columns:
            raise Exception("PatientID not added to columns")

    def _get_expected_column_data_from_xml(self, tags):
        expected_column_data = []
        metadata_pandas = self.dicom.metadata.download()
        for index, row in metadata_pandas.iterrows():
            metadata = row["metadata"].encode("ascii", "ignore")
            xml_root = etree.fromstring(metadata)
            expected_row = []
            for tag in tags:
                tag_query = ".//DicomAttribute[@keyword='" + tag + "']/Value/text()"
                query_result = xml_root.xpath(tag_query)
                if not query_result:
                    query_result = None
                else:
                    query_result = query_result[0].decode("ascii", "ignore")
                expected_row.append(query_result)
            expected_column_data.append(expected_row)
        return expected_column_data


if __name__ == "__main__":
    unittest.main()
