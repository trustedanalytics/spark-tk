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
        """test extract tag one col"""
        # generate our expected result using keywords
        # we will also get the tag numbers for the keywords
        expected_result, equivalent_tags = self._get_expected_column_data_from_xml(["PatientID"])
        self.dicom.extract_tags(equivalent_tags)

        columns = self.dicom.metadata.column_names
        for tag in equivalent_tags:
            if tag not in columns:
                raise Exception("tag was not added to columns")

        take_result = self.dicom.metadata.take(self.count, columns=equivalent_tags)
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_multiple_columns_basic(self):
        """test extract tag multiple cols"""
        # generate our expected result using keywords
        # we also get the equivalent tags for our keywords
        keywords = ["PatientID", "SOPInstanceUID"]
        expected_result, equivalent_tags = self._get_expected_column_data_from_xml(keywords)

        self.dicom.extract_tags(equivalent_tags)

        columns = self.dicom.metadata.column_names
        for tag in equivalent_tags:
            if tag not in columns:
                raise Exception("tag was not added to columns")

        take_result = self.dicom.metadata.take(self.count, columns=equivalent_tags)
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_invalid_column(self):
        """test extract tag invalid col"""
        self.dicom.extract_tags(["invalid"])

        columns = self.dicom.metadata.column_names
        if u'invalid' not in columns:
            raise Exception("Invalid column not added")

        invalid_column = self.dicom.metadata.take(self.count, columns=['invalid'])
        expected_result = [[None] for x in range(0, self.count)]
        self.assertEqual(invalid_column, expected_result)

    def test_extract_multiple_invalid_columns(self):
        """test extract tag mult invalid tags"""
        self.dicom.extract_tags(["invalid", "another_invalid_col"])

        columns = self.dicom.metadata.column_names
        if u'invalid' not in columns:
            raise Exception("invalid column not added to columns")
        if u'another_invalid_col' not in columns:
            raise Exception("another_invalid_col not added to columns")

        invalid_columns = self.dicom.metadata.take(self.count, columns=['invalid', 'another_invalid_col'])
        expected_result = [[None, None] for x in range(0, self.count)]
        self.assertEqual(invalid_columns, expected_result)

    def test_extract_invalid_valid_col_mix(self):
        """test extract tag mix of invalid and valid tags"""
        expected_result, equivalent_tags = self._get_expected_column_data_from_xml(["PatientID", "invalid"])
        equivalent_tags.append("invalid")

        self.dicom.extract_tags(equivalent_tags)

        columns = self.dicom.metadata.column_names
        if equivalent_tags[0] not in columns:
            raise Exception("tag not added to columns")
        if u'invalid' not in columns:
            raise Exception("invalid column not added to columns")

        take_result = self.dicom.metadata.take(self.count, columns=equivalent_tags)
        numpy.testing.assert_equal(take_result, expected_result)

    def test_extract_invalid_type(self):
        """test extract tags with invalid param type"""
        with self.assertRaisesRegexp(Exception, "should be either str or list"):
            self.dicom.extract_tags(1)

    def _get_expected_column_data_from_xml(self, keywords):
        # we will generate the expected result by
        # extracting the col data ourselves
        expected_column_data = []
        # we also keep trag of the tag numbers for the
        # keywords so we can give them to the extract_tags
        # function
        equivalent_tags = []

        # download the frame to pandas for ease of accesss
        metadata_pandas = self.dicom.metadata.to_pandas()

        # iterate through the metadata rows
        for index, row in metadata_pandas.iterrows():
            # convert to ascii string from unicode
            metadata = row["metadata"].encode("ascii", "ignore")
            # create a lxml etree of the data for xml search ops
            xml_root = etree.fromstring(metadata)

            expected_row = []
            for keyword in keywords:
                # search query for xml information
                # means get all of the DicomAttribute tags with our keyword
                keyword_query = "DicomAttribute[@keyword='" + keyword + "']"
                query_result = xml_root.xpath(keyword_query)

                # in the query result is [] use None instead
                if not query_result:
                    value = None
                else:
                    # get the inner text inside a value tag inside
                    # our keyword result
                    value = query_result[0].xpath("Value/text()")
                    # get the tag number
                    equivalent_tag = query_result[0].get("tag")
                    if equivalent_tag not in equivalent_tags:
                        equivalent_tags.append(equivalent_tag)
                    value = value[0].decode("ascii", "ignore")
                expected_row.append(value)
            expected_column_data.append(expected_row)
        return expected_column_data, equivalent_tags


if __name__ == "__main__":
    unittest.main()
