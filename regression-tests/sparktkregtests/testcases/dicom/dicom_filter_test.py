"""tests dicom.filter functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import os
import dicom
import numpy
import random
from lxml import etree
import datetime


class DicomFilterTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(DicomFilterTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = "../../../datasets/dicom/dicom_uncompressed/xml/"
        self.image_directory = "../../../datasets/dicom/dicom_uncompressed/imagedata/"
        self.query = ".//DicomAttribute[@keyword='KEYWORD']/Value/text()"
        self.count = self.dicom.metadata.count()

    def test_filter_one_key(self):
        first_row = self.dicom.metadata.download()["metadata"][0]
        xml = etree.fromstring(first_row.encode("ascii", "ignore"))
        patient_id = xml.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        self.dicom.filter(self._filter_key_values({ "PatientID" : patient_id }))

        expected_result = self._filter({ "PatientID" : patient_id })

        self._compare_dicom_with_expected_result(expected_result)

    def test_filter_multi_key(self):
        first_row = self.dicom.metadata.download()["metadata"][0]
        xml = etree.fromstring(first_row.encode("ascii", "ignore"))
        patient_id = xml.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        sopi_id = xml.xpath(self.query.replace("KEYWORD", "SOPInstanceUID"))[0]
        key_val = { "PatientID" : patient_id, "SOPInstanceUID" : sopi_id }

        self.dicom.filter(self._filter_key_values(key_val))
        expected_result = self._filter(key_val)

        self._compare_dicom_with_expected_result(expected_result)

    def test_filter_zero_matching_records(self):
        pandas = self.dicom.metadata.download()
        self.dicom.filter(self._filter_key_values({ "PatientID" : -6 }))
        self.assertEqual(0, self.dicom.metadata.count())

    def test_filter_nothing(self):
        self.dicom.filter(self._filter_nothing())
        self.assertEqual(self.dicom.metadata.count(), self.count)

    def test_filter_everything(self):
        self.dicom.filter(self._filter_everything())
        self.assertEqual(0, self.dicom.metadata.count())

    def test_filter_timestamp_range(self):
        begin_date = datetime.datetime.now() - datetime.timedelta(days=15*365)
        end_date = datetime.datetime.now() - datetime.timedelta(days=5*365)

        expected_result = []
        pandas = self.dicom.metadata.download()
        for index, row in pandas.iterrows():
            ascii_row = row["metadata"].encode("ascii", "ignore")
            xml_root = etree.fromstring(ascii_row)
            study_date = xml_root.xpath(self.query.replace("KEYWORD", "StudyDate"))[0]
            datetime_study_date = datetime.datetime.strptime(study_date, "%Y%m%d")
            if datetime_study_date > begin_date and datetime_study_date < end_date:
                expected_result.append(ascii_row)
        
        self.dicom.filter(self._filter_timestamp_range(begin_date, end_date))
        
        self._compare_dicom_with_expected_result(expected_result)

    def test_return_type_str(self):
        with self.assertRaisesRegexp(Exception, "'NoneType' object is not callable"):
            self.dicom.filter(self._filter_return_string())
            self.dicom.metadata.count()

    def test_return_type_int(self):
        with self.assertRaisesRegexp(Exception, "'NoneType' object is not callable"):
            self.dicom.filter(self._filter_return_int())
            self.dicom.metadata.count()

    def test_filter_has_bugs(self):
        with self.assertRaisesRegexp(Exception, "'NoneType' object is not callable"):
            self.dicom.filter(self._filter_has_bugs())
            self.dicom.metadata.count()

    def _filter_key_values(self, key_val):
        def _filter_key_value(row):
            metadata = row["metadata"].encode("ascii", "ignore")
            xml_root = etree.fromstring(metadata)
            for key in key_val:
                xml_element_value = xml_root.xpath(".//DicomAttribute[@keyword='" + key + "']/Value/text()")[0]
                if xml_element_value != key_val[key]:
                    return False
                else:
                    return True
        return _filter_key_value

    def _filter_nothing(self):
        def _filter_nothing(row):
            return True
        return _filter_nothing

    def _filter_everything(self):
        def _filter_everything(row):
            return False
        return _filter_everything

    def _filter_timestamp_range(self, begin_date, end_date):
        def _filter_timestamp_range(row):
            metadata = row["metadata"].encode("ascii", "ignore")
            xml_root = etree.fromstring(metadata)
            timestamp = xml_root.xpath(".//DicomAttribute[@keyword='StudyDate']/Value/text()")[0]
            timestamp = datetime.datetime.strptime(timestamp, "%Y%m%d")
            if begin_date < timestamp and timestamp < end_date:
                return True
            else:
                return False
        return _filter_timestamp_range

    def _filter_return_string(self):
        def _filter_return_string(row):
            return "True"

    def _filter_return_int(self):
        def _filter_return_int(row):
            return 1

    def _filter_has_bugs(self):
        def _filter_has_bugs(row):
            raise Exception("this filter is broken!")

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
                
    def _compare_dicom_with_expected_result(self, expected_result):
        pandas_result = self.dicom.metadata.download()["metadata"]
        for expected, actual in zip(expected_result, pandas_result):
            actual_ascii = actual.encode("ascii", "ignore")
            self.assertEqual(actual_ascii, expected)


if __name__ == "__main__":
    unittest.main()
