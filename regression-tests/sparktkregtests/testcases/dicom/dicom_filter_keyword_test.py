"""tests dicom.filter functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import os
import dicom
import numpy
import random
from lxml import etree


class DicomFilterKeywordsTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(DicomFilterKeywordsTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = "../../../datasets/dicom/dicom_uncompressed/xml/"
        self.image_directory = "../../../datasets/dicom/dicom_uncompressed/imagedata/"
        self.query = ".//DicomAttribute[@keyword='KEYWORD']/Value/text()"

    def test_filter_one_column_one_result_basic(self):
        metadata = self.dicom.metadata.download()
        random_row_index = random.randint(0, self.dicom.metadata.count() - 1)
        random_row = metadata["metadata"][random_row_index]
        xml_data = etree.fromstring(random_row.encode("ascii", "ignore"))
        random_row_sopi_id = xml_data.xpath(self.query.replace("KEYWORD", "SOPInstanceUID"))[0]

        self.dicom.filter_by_keywords({"SOPInstanceUID" : random_row_sopi_id })
        self.assertEqual(self.dicom.metadata.count(), 1)
        record = self.dicom.metadata.take(1)
        self.assertEqual(str(random_row), str(record))

    def test_filter_one_col_multi_result_basic(self):
        """test filter by keyword with one keyword mult record result"""
        metadata = self.dicom.metadata.download()
        first_row = metadata["metadata"][0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        first_row_patient_id = xml_data.xpath(self.query.replace("KEYWORD", "PatientID"))[0]

        expected_result = self._filter({"PatientID" : first_row_patient_id })

        self.dicom.filter_by_keywords({"PatientID" : first_row_patient_id })
        pandas_result = self.dicom.metadata.download()["metadata"]

        self.assertEqual(len(expected_result), self.dicom.metadata.count())
        for record, filtered_record in zip(records, pandas_result):
            self.assertEqual(record, filtered_record.encode("ascii", "ignore"))


    def test_filter_multiple_columns_basic(self):
        keyword_filter = {}
        metadata = self.dicom.metadata.download()["metadata"]
        first_row = metadata[0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        first_row_patient_id = xml_data.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        first_row_institution_name = xml_data.xpath(self.query.replace("KEYWORD", "InstitutionName"))[0]
        keyword_filter["PatientID"] = first_row_patient_id
        keyword_filter["InstitutionName"] = first_row_institution_name

        matching_records = self._filter(keyword_filter)

        self.dicom.filter_by_keywords(keyword_filter)
        pandas_result = self.dicom.metadata.download()["metadata"]

        self.assertEqual(len(matching_records), self.dicom.metadata.count())
        for expected_record, actual_record in zip(matching_records, pandas_result):
            ascii_actual_result = actual_record.encode("ascii", "ignore")
            self.assertEqual(ascii_actual_result, expected_record)

    def test_filter_invalid_column(self):
        self.dicom.filter_by_keywords({ "invalid keyword" : "value" })
        self.assertEqual(0, self.dicom.metadata.count())

    def test_filter_multiple_invalid_columns(self):
        self.dicom.filter_by_keywords({ "invalid" : "bla", "another_invalid_col" : "bla" })
        self.assertEqual(0, self.dicom.metadata.count())
           
    def test_filter_invalid_valid_col_mix(self):
        first_row = self.dicom.metadata.download()["metadata"][0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        patient_id = xml_data.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        self.dicom.filter_by_keywords({ "PatientID" : patient_id, "Invalid" : "bla" })
        self.assertEqual(0, self.dicom.metadata.count())

    def test_filter_invalid_type(self):
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.dicom.filter_by_keywords(1)

    def test_filter_unicode_columns(self):
        """test filter by keyword with one keyword mult record result"""
        metadata = self.dicom.metadata.download()
        first_row = metadata["metadata"][0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        first_row_patient_id = xml_data.xpath(self.query.replace("KEYWORD", "PatientID"))[0]

        expected_result = self._filter({ "PatientID" : first_row_patient_id })

        self.dicom.filter_by_keywords({ u'PatientID' : first_row_patient_id })
        pandas_result = self.dicom.metadata.download()["metadata"]

        self.assertEqual(len(expected_result), self.dicom.metadata.count())
        for record, filtered_record in zip(records, pandas_result):
            self.assertEqual(record, filtered_record.encode("ascii", "ignore"))

    def _filter(self, keywords):
        matching_records = []

        pandas_metadata = self.dicom.metadata.download()["metadata"]
        for row in pandas_metadata:
            ascii_xml = row.encode("ascii", "ignore")
            xml = etree.fromstring(row.encode("ascii", "ignore"))
            for keyword in keywords:
                this_row_keyword_value = xml.xpath(self.query.replace("KEYWORD", keyword))
                if this_row_keyword_value == keyword:
                    matching_records.append(ascii_xml)

        return matching_records
                


if __name__ == "__main__":
    unittest.main()
