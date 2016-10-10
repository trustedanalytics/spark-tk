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
        """test filter with basic filter function"""
        # extract a key-value pair from the first row metadata for our use
        first_row = self.dicom.metadata.to_pandas()["metadata"][0]
        xml = etree.fromstring(first_row.encode("ascii", "ignore"))
        patient_id = xml.xpath(self.query.replace("KEYWORD", "PatientID"))[0]

        # ask dicom to filter using our key-value filter function
        self.dicom.filter(self._filter_key_values({ "PatientID" : patient_id }))

        # we generate our own result to compare to dicom's
        expected_result = self._filter({ "PatientID" : patient_id })

        # ensure results match
        self._compare_dicom_with_expected_result(expected_result)

    def test_filter_multi_key(self):
        """test filter with basic filter function mult keyval pairs"""
        # first we extract key-value pairs from the first row's metadata
        # for our own use to generate a key-val dictionary
        first_row = self.dicom.metadata.to_pandas()["metadata"][0]
        xml = etree.fromstring(first_row.encode("ascii", "ignore"))
        patient_id = xml.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        sopi_id = xml.xpath(self.query.replace("KEYWORD", "SOPInstanceUID"))[0]
        key_val = { "PatientID" : patient_id, "SOPInstanceUID" : sopi_id }

        # we use our filter function and ask dicom to filter
        self.dicom.filter(self._filter_key_values(key_val))

        # here we generate our own result
        expected_result = self._filter(key_val)

        # compare expected result to what dicom gave us
        self._compare_dicom_with_expected_result(expected_result)

    def test_filter_zero_matching_records(self):
        """test filter with filter function returns none"""
        # we give dicom a filter function which filters by
        # key-value and give it a key-value pair which will
        # return 0 records
        pandas = self.dicom.metadata.to_pandas()
        self.dicom.filter(self._filter_key_values({ "PatientID" : -6 }))
        self.assertEqual(0, self.dicom.metadata.count())

    def test_filter_nothing(self):
        """test filter with filter function filters nothing"""
        # this filter function will return all records
        self.dicom.filter(self._filter_nothing())
        self.assertEqual(self.dicom.metadata.count(), self.count)

    def test_filter_everything(self):
        """test filter function filter everything"""
        # filter_everything filter out all of the records
        self.dicom.filter(self._filter_everything())
        self.assertEqual(0, self.dicom.metadata.count())

    def test_filter_timestamp_range(self):
        """test filter with timestamp range function"""
        # we will test filter with a function which takes a begin and end
        # date and returns all records with a study date between them
        # we will set begin date to 15 years ago and end date to 5 years ago
        begin_date = datetime.datetime.now() - datetime.timedelta(days=15*365)
        end_date = datetime.datetime.now() - datetime.timedelta(days=5*365)

        # here we will generate our own result by filtering for records
        # which meet our criteria
        expected_result = []
        pandas = self.dicom.metadata.to_pandas()
        # iterate through the rows and append all records with
        # a study date between our begin and end date
        for index, row in pandas.iterrows():
            ascii_row = row["metadata"].encode("ascii", "ignore")
            xml_root = etree.fromstring(ascii_row)
            study_date = xml_root.xpath(self.query.replace("KEYWORD", "StudyDate"))[0]
            datetime_study_date = datetime.datetime.strptime(study_date, "%Y%m%d")
            if datetime_study_date > begin_date and datetime_study_date < end_date:
                expected_result.append(ascii_row)
        
        # now we ask dicom to use our filter function below to return
        # all records with a StudyDate within our specified range
        self.dicom.filter(self._filter_timestamp_range(begin_date, end_date))
        
        # ensure that expected result matches actual
        self._compare_dicom_with_expected_result(expected_result)

    def test_return_type_str(self):
        """test filter with function that returns strings"""
        with self.assertRaisesRegexp(Exception, "'NoneType' object is not callable"):
            self.dicom.filter(self._filter_return_string())
            self.dicom.metadata.count()

    def test_return_type_int(self):
        """test filter wtih function that returns ints"""
        with self.assertRaisesRegexp(Exception, "'NoneType' object is not callable"):
            self.dicom.filter(self._filter_return_int())
            self.dicom.metadata.count()

    def test_filter_has_bugs(self):
        """test filter with a broken filter function"""
        with self.assertRaisesRegexp(Exception, "this filter is broken!"):
            self.dicom.filter(self._filter_has_bugs())
            self.dicom.metadata.count()

    def test_filter_invalid_param(self):
        """test filter with an invalid param type"""
        # should fail because filter takes a function not a keyvalue pair
        with self.assertRaisesRegexp(Exception, "'dict' object is not callable"):
            self.dicom.filter({ "PatientID" : "bla" })
            self.dicom.metadata.count()

    def test_filter_invalid_function(self):
        """test filter with function which takes more than one param"""
        with self.assertRaisesRegexp(Exception, "takes exactly 2 arguments"):
            self.dicom.filter(self._filter_invalid())
            self.dicom.metadata.count()

    def _filter_key_values(self, key_val):
        """filter by key-value"""
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
        """returns all records"""
        def _filter_nothing(row):
            return True
        return _filter_nothing

    def _filter_everything(self):
        """returns no records"""
        def _filter_everything(row):
            return False
        return _filter_everything

    def _filter_timestamp_range(self, begin_date, end_date):
        """return records within studydate date range"""
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
        """filter function which returns str"""
        def _filter_return_string(row):
            return "True"
        return _filter_return_string

    def _filter_return_int(self):
        """filter function returns int"""
        def _filter_return_int(row):
            return -1
        return _filter_return_int

    def _filter_has_bugs(self):
        """broken filter function"""
        def _filter_has_bugs(row):
            raise Exception("this filter is broken!")
        return _filter_has_bugs

    def _filter_invalid(self):
        """filter function takes 2 params"""
        # filter is invalid because it takes
        # 2 parameters
        def _filter_invalid(index, row):
            return True
        return _filter_invalid

    def _filter(self, keywords):
        """filter records by key value pair"""
        # here we are generating the expected result
        matching_records = []

        pandas_metadata = self.dicom.metadata.to_pandas()["metadata"]
        for row in pandas_metadata:
            ascii_xml = row.encode("ascii", "ignore")
            xml = etree.fromstring(row.encode("ascii", "ignore"))
            for keyword in keywords:
                this_row_keyword_value = xml.xpath(self.query.replace("KEYWORD", keyword))
                if this_row_keyword_value == keyword:
                    matching_records.append(ascii_xml)

        return matching_records
                
    def _compare_dicom_with_expected_result(self, expected_result):
        """compare expected result with actual result"""
        pandas_result = self.dicom.metadata.to_pandas()["metadata"]
        for expected, actual in zip(expected_result, pandas_result):
            actual_ascii = actual.encode("ascii", "ignore")
            self.assertEqual(actual_ascii, expected)


if __name__ == "__main__":
    unittest.main()
