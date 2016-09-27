"""tests dicom.filter functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import numpy
from lxml import etree


class DicomExtractTagsTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(DicomExtractTagsTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = "../../../datasets/dicom/dicom_uncompressed/xml/"
        self.image_directory = "../../../datasets/dicom/dicom_uncompressed/imagedata/"
        self.count = self.dicom.metadata.count()

    def test_extract_one_column_basic(self):
        expected_result, equivalent_tags = self._get_expected_column_data_from_xml(["PatientID"])
        self.dicom.extract_tags(equivalent_tags)
        columns = self.dicom.metadata.column_names
        for tag in equivalent_tags:
            if tag not in columns:
                raise Exception("tag was not added to columns")
        take_result = self.dicom.metadata.take(self.count, columns=equivalent_tags).data
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_multiple_columns_basic(self):
        keywords = ["PatientID", "SOPInstanceUID"]
        expected_result, equivalent_tags = self._get_expected_column_data_from_xml(keywords)
        self.dicom.extract_tags(equivalent_tags)
        columns = self.dicom.metadata.column_names
        for tag in equivalent_tags:
            if tag not in columns:
                raise Exception("tag was not added to columns")
        take_result = self.dicom.metadata.take(self.count, columns=equivalent_tags).data
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_invalid_column(self):
        self.dicom.extract_tags(["invalid"])
        columns = self.dicom.metadata.column_names
        if u'invalid' not in columns:
            raise Exception("Invalid column not added")
        invalid_column = self.dicom.metadata.take(self.count, columns=[u'invalid']).data
        expected_result = [[None] for x in range(0, self.count)]
        self.assertEqual(invalid_column, expected_result)

    def test_extract_multiple_invalid_columns(self):
        self.dicom.extract_tags(["invalid", "another_invalid_col"])
        columns = self.dicom.metadata.column_names
        if u'invalid' not in columns:
            raise Exception("invalid column not added to columns")
        if u'another_invalid_col' not in columns:
            raise Exception("another_invalid_col not added to columns")
        invalid_columns = self.dicom.metadata.take(self.count, columns=[u'invalid', u'another_invalid_col']).data
        expected_result = [[None, None] for x in range(0, self.count)]
        self.assertEqual(invalid_columns, expected_result)

    def test_extract_invalid_valid_col_mix(self):
        expected_result, equivalent_tags = self._get_expected_column_data_from_xml(["PatientID", "invalid"])
        equivalent_tags.append("invalid")
        self.dicom.extract_tags(equivalent_tags)
        columns = self.dicom.metadata.column_names
        if equivalent_tags[0] not in columns:
            raise Exception("tag not added to columns")
        if u'invalid' not in columns:
            raise Exception("invalid column not added to columns")
        take_result = self.dicom.metadata.take(self.count, columns=equivalent_tags).data
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_invalid_type(self):
        with self.assertRaisesRegexp(Exception, "should be either str or list"):
            self.dicom.extract_tags(1)

    def _get_expected_column_data_from_xml(self, keywords):
        expected_column_data = []
        equivalent_tags = []
        metadata_pandas = self.dicom.metadata.download()
        for index, row in metadata_pandas.iterrows():
            metadata = row["metadata"].encode("ascii", "ignore")
            xml_root = etree.fromstring(metadata)
            expected_row = []
            for keyword in keywords:
                keyword_query = "DicomAttribute[@keyword='" + keyword + "']"
                query_result = xml_root.xpath(keyword_query)
                if not query_result:
                    value = None
                else:
                    value = query_result[0].xpath("Value/text()")
                    equivalent_tag = query_result[0].get("tag")
                    if equivalent_tag not in equivalent_tags:
                        equivalent_tags.append(equivalent_tag)
                    value = value[0].decode("ascii", "ignore")
                expected_row.append(value)
            expected_column_data.append(expected_row)
        return expected_column_data, equivalent_tags


if __name__ == "__main__":
    unittest.main()
