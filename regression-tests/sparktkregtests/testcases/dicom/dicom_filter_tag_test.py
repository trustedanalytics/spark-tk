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

"""tests dicom.filter functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import os
import dicom
import numpy
import random
from lxml import etree


class DicomFilterTagsTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(DicomFilterTagsTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = "../../../datasets/dicom/dicom_uncompressed/xml/"
        self.image_directory = "../../../datasets/dicom/dicom_uncompressed/imagedata/"
        self.query = ".//DicomAttribute[@keyword='KEYWORD']"

    @unittest.skip("sparktk: extract/filter by keyword/tag not working")
    def test_filter_one_column_one_result_basic(self):
        """test filter with one unique tag"""
        # get pandas frame for ease of access
        metadata = self.dicom.metadata.to_pandas()

        # get a random row and extract its sopinstanceuid for our use
        random_row_index = random.randint(0, self.dicom.metadata.count() - 1)
        random_row = metadata["metadata"][random_row_index]
        xml_data = etree.fromstring(random_row.encode("ascii", "ignore"))
        random_row_sopi_id = xml_data.xpath(self.query.replace("KEYWORD", "SOPInstanceUID"))[0]
        # get the tag number for the sopinstanceuid element
        tag_number = random_row_sopi_id.get("tag")
        # get the corresponding value
        tag_value = random_row_sopi_id.xpath("/Value/text()")

        # ask dicom to filter by the tag-value pair we extracted
        self.dicom.filter_by_tags({ tag_number : tag_value })

        # since sopinstanceuid is supposed to be unique dicom should have
        # returned only one record, the record which we randomly selected above
        self.assertEqual(self.dicom.metadata.count(), 1)
        record = self.dicom.metadata.take(1)
        self.assertEqual(str(random_row), str(record))

    @unittest.skip("sparktk: extract/filter by keyword/tag not working")
    def test_filter_one_col_multi_result_basic(self):
        """test filter by tag with one tag mult record result"""
        metadata = self.dicom.metadata.to_pandas()

        # get the first row and extract the patient id element from the metadata xml
        first_row = metadata["metadata"][0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        first_row_patient_id = xml_data.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        # get the tag number for the patient id xml element
        tag_number = first_row_patient_id.get("tag")
        # get the value for that tag
        tag_value = first_row_patient_id.xpath("/Value/text()")

        # we filter ourselves to get the expected result for this key value pair
        expected_result = self._filter({"PatientID" : first_row_patient_id })

        # ask dicom to filter by tag, giving the tag-value pair
        self.dicom.filter_by_tags({ tag_number  : first_row_patient_id })

        # compare our result to dicom's
        pandas_result = self.dicom.metadata.to_pandas()["metadata"]
        self.assertEqual(len(expected_result), self.dicom.metadata.count())
        for record, filtered_record in zip(records, pandas_result):
            self.assertEqual(record, filtered_record.encode("ascii", "ignore"))

    @unittest.skip("sparktk: extract/filter by keyword/tag not working")
    def test_filter_multiple_columns_basic(self):
        """test filter tags with multiple tags"""
        # here we will generate our filter
        keyword_filter = {}

        # we will get the first row and extract the patient id and institution name
        metadata = self.dicom.metadata.to_pandas()["metadata"]
        first_row = metadata[0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        first_row_patient_id = xml_data.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        first_row_institution_name = xml_data.xpath(self.query.replace("KEYWORD", "InstitutionName"))[0]
        # get the tag numbers and values
        patient_id_tag = first_row_patient_id.get("tag")
        patient_id_value = first_row_patient_id.xpath("/Value/text()")
        institution_tag = first_row_institution_name.get("tag")
        institution_value = first_row_institution_name.xpath("/Value/text()")
        keyword_filter["PatientID"] = patient_id_value
        keyword_filter["InstitutionName"] = institution_value

        # we do the filtering ourselves to generate the expected result
        matching_records = self._filter(keyword_filter)

        # we ask dicom to filter by tag with the tag-value pairs we extracted
        self.dicom.filter_by_tags({ patient_id_tag : patient_id_value, institution_tag : institution_value })
        pandas_result = self.dicom.metadata.to_pandas()["metadata"]

        # finally we ensure dicom's result matches ours
        self.assertEqual(len(matching_records), self.dicom.metadata.count())
        for expected_record, actual_record in zip(matching_records, pandas_result):
            ascii_actual_result = actual_record.encode("ascii", "ignore")
            self.assertEqual(ascii_actual_result, expected_record)

    @unittest.skip("sparktk: extract/filter by keyword/tag not working")
    def test_filter_invalid_column(self):
        """test filter tags with invalid tag name"""
        self.dicom.filter_by_tags({ "invalid keyword" : "value" })
        self.assertEqual(0, self.dicom.metadata.count())

    @unittest.skip("sparktk: extract/filter by keyword/tag not working")
    def test_filter_multiple_invalid_columns(self):
        """test filter tags with mult invalid tag names"""
        self.dicom.filter_by_tags({ "invalid" : "bla", "another_invalid_col" : "bla" })
        self.assertEqual(0, self.dicom.metadata.count())

    @unittest.skip("sparktk: extract/filter by keyword/tag not working")      
    def test_filter_invalid_valid_col_mix(self):
        """test filter tags with a mix of valid and invalid tags"""
        # first we will extract a valid tag number and value from the xml
        first_row = self.dicom.metadata.to_pandas()["metadata"][0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        patient_id = xml_data.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        patient_id_tag = patient_id.get("tag")
        patient_id_value = patient_id.xpath("/Value/text()")

        # now we ask dicom to filter by tags giving it both the valid tag-value
        # pair we extracted and also an invalid tag-value pair
        self.dicom.filter_by_tags({ patient_id_tag : patient_id_value, "Invalid" : "bla" })
        
        # since zero records match both criteria dicom should return no records
        self.assertEqual(0, self.dicom.metadata.count())

    @unittest.skip("sparktk: improper filter does not give useful error in dicom.filter")
    def test_filter_invalid_type(self):
        """test filter tags with invalid param type"""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.dicom.filter_by_tags(1)

    def _filter(self, keywords):
        """filter the dicom records with key-value pairs"""
        # we are going to generate the expected results for the
        # key-value filter
        matching_records = []

        pandas_metadata = self.dicom.metadata.to_pandas()["metadata"]

        # iterate through the records and return any records which
        # match our key value pair criteria
        for row in pandas_metadata:
            ascii_xml = row.encode("ascii", "ignore")
            xml = etree.fromstring(row.encode("ascii", "ignore"))
            for keyword in keywords:
                this_row_keyword_value = xml.xpath(self.query.replace("KEYWORD", keyword))
                if this_row_keyword_value == keywords[keyword]:
                    matching_records.append(ascii_xml)

        return matching_records
                


if __name__ == "__main__":
    unittest.main()
