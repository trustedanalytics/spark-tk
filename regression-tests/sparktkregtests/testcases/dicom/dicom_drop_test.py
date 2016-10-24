# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""tests dicom.drop functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import os
import dicom
import numpy
import random
from lxml import etree
import datetime


class DicomDropTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(DicomDropTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = "../../../datasets/dicom/dicom_uncompressed/xml/"
        self.image_directory = "../../../datasets/dicom/dicom_uncompressed/imagedata/"
        self.query = ".//DicomAttribute[@keyword='KEYWORD']/Value/text()"
        self.count = self.dicom.metadata.count()

    def test_drop_one_key(self):
        """test drop with basic drop function"""
        # we are going to identify the patient id for a row in our dicom metadata
        # this way we have a real-time key-value pair to use to give our drop function
        first_row = self.dicom.metadata.to_pandas()["metadata"][0]
        xml = etree.fromstring(first_row.encode("ascii", "ignore"))
        patient_id = xml.xpath(self.query.replace("KEYWORD", "PatientID"))[0]

        # ask dicom to drop using our key-value filter function
        self.dicom.drop_rows(self._drop_key_values({ "PatientID" : patient_id }))

        # we generate our own result to compare to dicom's
        expected_result = self._drop({ "PatientID" : patient_id })

        # ensure results match
        self._compare_dicom_with_expected_result(expected_result)

    def test_drop_multi_key(self):
        """test drop with basic filter function mult keyval pairs"""
        # first we extract key-value pairs from the first row's metadata
        # for our own use to generate a key-val dictionary
        first_row = self.dicom.metadata.to_pandas()["metadata"][0]
        xml = etree.fromstring(first_row.encode("ascii", "ignore"))
        patient_id = xml.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        sopi_id = xml.xpath(self.query.replace("KEYWORD", "SOPInstanceUID"))[0]
        key_val = { "PatientID" : patient_id, "SOPInstanceUID" : sopi_id }

        # we use our drop function and ask dicom to filter
        self.dicom.drop_rows(self._drop_key_values(key_val))

        # here we generate our own result
        expected_result = self._drop(key_val)

        # compare expected result to what dicom gave us
        self._compare_dicom_with_expected_result(expected_result)

    def test_drop_zero_matching_records(self):
        """test drop with filter function returns none"""
        # we give dicom a drop function which filters by
        # key-value and give it a key-value pair which will
        # return 0 records
        pandas = self.dicom.metadata.to_pandas()
        self.dicom.drop_rows(self._drop_key_values({ "PatientID" : -6 }))
        self.assertEqual(3, self.dicom.metadata.count())

    def test_drop_everything(self):
        """test drop with filter function filters nothing"""
        # this drop function will return all records
        self.dicom.drop_rows(self._drop_nothing())
        self.assertEqual(self.dicom.metadata.count(), 0)

    def test_nothing(self):
        """test drop function filter everything"""
        # drop_everything filter out all of the records
        self.dicom.drop_rows(self._drop_everything())
        self.assertEqual(self.count, self.dicom.metadata.count())

    def test_drop_timestamp_range(self):
        """test drop with timestamp range function"""
        # we will test drop with a function which takes a begin and end
        # date and returns all records with a study date between them
        # we will set begin date to 15 years ago and end date to 5 years ago
        begin_date = datetime.datetime.now() - datetime.timedelta(days=15*365)
        end_date = datetime.datetime.now() - datetime.timedelta(days=5*365)

        # here we will generate our own result by droping for records
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
            if datetime_study_date < begin_date or datetime_study_date > end_date:
                expected_result.append(ascii_row)
        
        # now we ask dicom to use our drop function below to return
        # all records with a StudyDate within our specified range
        self.dicom.drop_rows(self._drop_timestamp_range(begin_date, end_date))
        
        # ensure that expected result matches actual
        self._compare_dicom_with_expected_result(expected_result)

    def test_drop_drop_has_bugs(self):
        """test drop with a broken filter function"""
        with self.assertRaisesRegexp(Exception, "this drop is broken!"):
            self.dicom.drop_rows(self._drop_has_bugs())
            self.dicom.metadata.count()

    def test_drop_invalid_param(self):
        """test drop with an invalid param type"""
        # should fail because drop takes a function not a keyvalue pair
        with self.assertRaisesRegexp(Exception, "'dict' object is not callable"):
            self.dicom.drop_rows({ "PatientID" : "bla" })
            self.dicom.metadata.count()

    def test_drop_invalid_function(self):
        """test drop with function which takes more than one param"""
        with self.assertRaisesRegexp(Exception, "takes exactly 2 arguments"):
            self.dicom.drop_rows(self._drop_invalid())
            self.dicom.metadata.count()

    def _drop_key_values(self, key_val):
        """drop by key-value"""
        def _drop_key_value(row):
            metadata = row["metadata"].encode("ascii", "ignore")
            xml_root = etree.fromstring(metadata)
            for key in key_val:
                xml_element_value = xml_root.xpath(".//DicomAttribute[@keyword='" + key + "']/Value/text()")[0]
                if xml_element_value != key_val[key]:
                    return False
                else:
                    return True
        return _drop_key_value

    def _drop_nothing(self):
        """returns all records"""
        def _drop_nothing(row):
            return True
        return _drop_nothing

    def _drop_everything(self):
        """returns no records"""
        def _drop_everything(row):
            return False
        return _drop_everything

    def _drop_timestamp_range(self, begin_date, end_date):
        """return records within studydate date range"""
        def _drop_timestamp_range(row):
            metadata = row["metadata"].encode("ascii", "ignore")
            xml_root = etree.fromstring(metadata)
            timestamp = xml_root.xpath(".//DicomAttribute[@keyword='StudyDate']/Value/text()")[0]
            timestamp = datetime.datetime.strptime(timestamp, "%Y%m%d")
            if begin_date < timestamp and timestamp < end_date:
                return True
            else:
                return False
        return _drop_timestamp_range

    def _drop_return_string(self):
        """drop function which returns str"""
        def _drop_return_string(row):
            return "True"
        return _drop_return_string

    def _drop_return_int(self):
        """drop function returns int"""
        def _drop_return_int(row):
            return -1
        return _drop_return_int

    def _drop_has_bugs(self):
        """broken drop function"""
        def _drop_has_bugs(row):
            raise Exception("this drop is broken!")
        return _drop_has_bugs

    def _drop_invalid(self):
        """drop function takes 2 params"""
        # drop is invalid because it takes
        # 2 parameters
        def _drop_invalid(index, row):
            return True
        return _drop_invalid

    def _drop(self, keywords):
        """drop records by key value pair"""
        # here we are generating the expected result
        matching_records = []

        pandas_metadata = self.dicom.metadata.to_pandas()["metadata"]
        for row in pandas_metadata:
            ascii_xml = row.encode("ascii", "ignore")
            xml = etree.fromstring(row.encode("ascii", "ignore"))
            for keyword in keywords:
                keyword_search = xml.xpath(self.query.replace("KEYWORD", keyword))
                if len(keyword_search) != 0:
                    if this_row_keyword_value != keyword:
                        matching_records.append(ascii_xml)
                else:
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
