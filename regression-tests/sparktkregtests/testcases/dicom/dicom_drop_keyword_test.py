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

"""tests dicom.drop by keyword functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import numpy
import random
from lxml import etree


class DicomDropKeywordsTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(DicomDropKeywordsTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        # query for the text value of that keyword from xml
        self.query = ".//DicomAttribute[@keyword='KEYWORD']/Value/text()"
        self.count = self.dicom.metadata.count()

    def test_drop_one_column_one_result_basic(self):
        """test drop with one unique key"""
        # get the pandas frame for ease of access
        metadata = self.dicom.metadata.to_pandas()

        # grab a random row and extract the SOPInstanceUID from that record
        random_row_index = random.randint(0, self.dicom.metadata.count() - 1)
        random_row = metadata["metadata"][random_row_index]
        xml_data = etree.fromstring(random_row.encode("ascii", "ignore"))
        random_row_sopi_id = xml_data.xpath(self.query.replace(
            "KEYWORD", "SOPInstanceUID"))[0]
        expected_result = self._drop({"SOPInstanceUID": random_row_sopi_id})
        # get all of the records with our randomly selected sopinstanceuid
        # since sopinstanceuid is supposed to be unique for each record
        # we should only get back the record which we randomly selected above
        self.dicom.drop_rows_by_keywords(
            {"SOPInstanceUID": random_row_sopi_id})

        # check that our result is correct
        # we should have gotten back from drop the row
        # which we randomly selected
        self.assertEqual(self.dicom.metadata.count(), self.count - 1)
        self._compare_dicom_with_expected_result(expected_result)

    def test_drop_one_col_multi_result_basic(self):
        """test drop by keyword with one keyword mult record result"""
        # get pandas frame for ease of access
        metadata = self.dicom.metadata.to_pandas()

        # grab a random row and extract the patient id
        first_row = metadata["metadata"][0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        first_row_patient_id = xml_data.xpath(self.query.replace(
            "KEYWORD", "PatientID"))[0]

        # drop the records ourselves to get the expected result
        expected_result = self._drop({"PatientID": first_row_patient_id})

        # get all of the records with that patient id
        self.dicom.drop_rows_by_keywords({"PatientID": first_row_patient_id})

        # get the pandas frame for ease of access
        pandas_result = self.dicom.metadata.to_pandas()["metadata"]

        # ensure that our expected result matches what dicom returned
        self.assertEqual(len(
            expected_result["metadata"]), self.dicom.metadata.count())
        self.assertEqual(len(
            expected_result["pixeldata"]), self.dicom.pixeldata.count())
        for record, droped_record in zip(expected_result, pandas_result):
            self.assertEqual(record, droped_record.encode("ascii", "ignore"))

    def test_drop_multiple_columns_basic(self):
        """test drop with multiple key vals"""
        # first we will generate a drop randomly by
        # randomly selecting a row and extracting values that we want to use
        keyword_drop = {}
        metadata = self.dicom.metadata.to_pandas()["metadata"]
        first_row = metadata[0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        first_row_patient_id = xml_data.xpath(self.query.replace(
            "KEYWORD", "PatientID"))[0]
        first_row_institution_name = xml_data.xpath(self.query.replace(
            "KEYWORD", "BodyPartExamined"))[0]
        keyword_drop["PatientID"] = first_row_patient_id
        keyword_drop["BodyPartExamined"] = first_row_institution_name

        # now we generate our expected result by droping ourselves
        matching_records = self._drop(keyword_drop)

        # get the records which match our drop
        self.dicom.drop_rows_by_keywords(keyword_drop)
        pandas_result = self.dicom.metadata.to_pandas()["metadata"]

        # finally we check to ensure that dicom's
        # result matches our expected result
        self.assertEqual(len(
            matching_records["metadata"]), self.dicom.metadata.count())
        self.assertEqual(len(
            matching_records["pixeldata"]), self.dicom.pixeldata.count())
        for expected_record, actual_record in zip(
                matching_records, pandas_result):
            ascii_actual_result = actual_record.encode("ascii", "ignore")
            self.assertEqual(ascii_actual_result, expected_record)

    def test_drop_invalid_column(self):
        """test drop invalid key"""
        self.dicom.drop_rows_by_keywords({"invalid keyword": "value"})
        self.assertEqual(self.count, self.dicom.metadata.count())
        self.assertEqual(self.count, self.dicom.pixeldata.count())

    def test_drop_multiple_invalid_columns(self):
        """test drop mult invalid keys"""
        self.dicom.drop_rows_by_keywords(
            {"invalid": "bla", "another_invalid_col": "bla"})
        self.assertEqual(self.count, self.dicom.metadata.count())
        self.assertEqual(self.count, self.dicom.pixeldata.count())

    def test_valid_keyword_zero_results(self):
        """test drop with key-value pair, key exists but no matches"""
        self.dicom.drop_rows_by_keywords({"SOPInstanceUID": "2"})
        self.assertEqual(self.count, self.dicom.metadata.count())
        self.assertEqual(self.count, self.dicom.pixeldata.count())

    def test_drop_invalid_valid_col_mix(self):
        """test drop with mix of valid and invalid keys"""
        # first we get a valid patient id by selecting the first row
        # and extracting its patient id
        first_row = self.dicom.metadata.to_pandas()["metadata"][0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        patient_id = xml_data.xpath(self.query.replace(
            "KEYWORD", "PatientID"))[0]

        # now we ask dicom to drop using a filter which is a mix of a valid
        # key-value pair and an invalid key-value pair
        self.dicom.drop_rows_by_keywords(
            {"PatientID": patient_id, "Invalid": "bla"})

        # since there are no records which meet BOTH key value criterias
        # we assert that 0 records were returned
        self.assertEqual(self.count, self.dicom.metadata.count())
        self.assertEqual(self.count, self.dicom.pixeldata.count())

    def test_drop_invalid_type(self):
        """test drop invalid param type"""
        with self.assertRaisesRegexp(Exception, "incomplete format"):
            self.dicom.drop_rows_by_keywords(1)
            self.dicom.metadata.count()

    def test_drop_unicode_columns(self):
        """test drop by keyword with unicode keys"""
        # the logic is the same as test_drop_one_column above
        # the only difference is here we are giving the keys as unicode
        # strings instead of standard python strings
        metadata = self.dicom.metadata.to_pandas()
        first_row = metadata["metadata"][0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        first_row_patient_id = xml_data.xpath(self.query.replace(
            "KEYWORD", "PatientID"))[0]

        expected_result = self._drop({"PatientID": first_row_patient_id})

        self.dicom.drop_rows_by_keywords({u'PatientID': first_row_patient_id})
        pandas_result = self.dicom.metadata.to_pandas()["metadata"]

        self.assertEqual(len(
            expected_result["metadata"]), self.dicom.metadata.count())
        self.assertEqual(len(
            expected_result["pixeldata"]), self.dicom.pixeldata.count())
        for record, droped_record in zip(expected_result, pandas_result):
            self.assertEqual(record, droped_record.encode("ascii", "ignore"))

    def _drop(self, keywords):
        """generate our expected result by droping the records"""
        # here we are generating the expected result from the key-value
        # drop so that we can compare it to what dicom returns
        # we will iterate through the dicom metadata to get all of the
        # records which match our key-value criteria
        matching_metadata = []
        matching_pixeldata = []

        pandas_metadata = self.dicom.metadata.to_pandas()["metadata"]
        pandas_pixeldata = self.dicom.pixeldata.to_pandas()["imagematrix"]
        for (metadata, pixeldata) in zip(pandas_metadata, pandas_pixeldata):
            ascii_xml = metadata.encode("ascii", "ignore")
            xml = etree.fromstring(ascii_xml)
            for keyword in keywords:
                this_row_keyword_value = xml.xpath(self.query.replace(
                    "KEYWORD", keyword))[0]
                if this_row_keyword_value != keywords[keyword]:
                    matching_metadata.append(ascii_xml)
                    matching_pixeldata.append(pixeldata)

        return {"metadata": matching_metadata, "pixeldata": matching_pixeldata}

    def _compare_dicom_with_expected_result(self, expected_result):
        """compare expected result with actual result"""
        pandas_metadata = self.dicom.metadata.to_pandas()["metadata"]
        pandas_pixeldata = self.dicom.pixeldata.to_pandas()["imagematrix"]
        for expected, actual in zip(
                expected_result["metadata"], pandas_metadata):
            actual_ascii = actual.encode("ascii", "ignore")
            self.assertEqual(actual_ascii, expected)
        for expected, actual in zip(
                expected_result["pixeldata"], pandas_pixeldata):
            numpy.testing.assert_equal(expected, actual)


if __name__ == "__main__":
    unittest.main()
