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

"""tests dicom.drop by tag functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import numpy
import random
from lxml import etree


class DicomDropTagsTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(DicomDropTagsTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = "../../../datasets/dicom/dicom_uncompressed/xml/"
        self.image_directory = "../../../datasets/dicom/dicom_uncompressed/imagedata/"
        self.query = ".//DicomAttribute[@keyword='KEYWORD']/Value/text()"
        self.element_query = ".//DicomAttribute[@keyword='KEYWORD']"
        self.count = self.dicom.metadata.count()

    def test_drop_one_column_one_result_basic(self):
        """test drop with one unique key"""
        # get the pandas frame for ease of access
        metadata = self.dicom.metadata.to_pandas()
        # grab a random row and extract the SOPInstanceUID from that record
        random_row_index = random.randint(0, self.dicom.metadata.count() - 1)
        random_row = metadata["metadata"][random_row_index]
        xml_data = etree.fromstring(random_row.encode("ascii", "ignore"))
        random_row_sopi_id = xml_data.xpath(self.query.replace("KEYWORD", "SOPInstanceUID"))[0]
        expected_result = self._drop({"SOPInstanceUID": random_row_sopi_id})
        # get all of the records with our randomly selected sopinstanceuid
        # since sopinstanceuid is supposed to be unique for each record
        # we should only get back the record which we randomly selected above
        tag_number = xml_data.xpath(self.element_query.replace("KEYWORD", "SOPInstanceUID"))[0].get("tag")
        self.dicom.drop_rows_by_tags({tag_number: random_row_sopi_id})

        # check that our result is correct
        # we should have gotten back from drop the row
        # which we randomly selected
        self.assertEqual(self.dicom.metadata.count(), self.count - 1)
        self._compare_dicom_with_expected_result(expected_result)

    def test_drop_one_col_multi_result_basic(self):
        """test drop by tag with one tag mult record result"""
        metadata = self.dicom.metadata.to_pandas()

        # get the first row and extract the patient id element from the metadata xml
        first_row = metadata["metadata"][0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        first_row_patient_id = xml_data.xpath(self.query.replace("KEYWORD", "PatientID"))[0]

        # we drop ourselves to get the expected result for this key value pair
        expected_result = self._drop({"PatientID": first_row_patient_id})
        # ask dicom to drop by tag, giving the tag-value pair
        tag_number = xml_data.xpath(self.element_query.replace("KEYWORD", "PatientID"))[0].get("tag")
        self.dicom.drop_rows_by_tags({tag_number: first_row_patient_id})

        # compare our result to dicom's
        self._compare_dicom_with_expected_result(expected_result)

    def test_drop_multiple_columns_basic(self):
        """test drop tags with multiple tags"""
        # here we will generate our drop
        keyword_drop = {}

        # we will get the first row and extract the patient id and institution name
        metadata = self.dicom.metadata.to_pandas()["metadata"]
        first_row = metadata[0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        first_row_patient_id = xml_data.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        first_row_body_part = xml_data.xpath(self.query.replace("KEYWORD", "BodyPartExamined"))[0]
        # get the tag numbers and values
        patient_id_tag = xml_data.xpath(self.element_query.replace("KEYWORD", "PatientID"))[0].get("tag")
        body_part_tag = xml_data.xpath(self.element_query.replace("KEYWORD", "BodyPartExamined"))[0].get("tag")
        keyword_drop["PatientID"] = first_row_patient_id
        keyword_drop["BodyPartExamined"] = first_row_body_part

        # we do the droping ourselves to generate the expected result
        matching_records = self._drop(keyword_drop)

        # we ask dicom to drop by tag with the tag-value pairs we extracted
        self.dicom.drop_rows_by_tags({patient_id_tag: first_row_patient_id, body_part_tag: first_row_body_part})

        # finally we ensure dicom's result matches ours
        self._compare_dicom_with_expected_result(matching_records)

    def test_drop_invalid_column(self):
        """test drop tags with invalid tag name"""
        self.dicom.drop_rows_by_tags({"invalid keyword": "value"})
        self.assertEqual(self.count, self.dicom.metadata.count())

    def test_drop_multiple_invalid_columns(self):
        """test drop tags with mult invalid tag names"""
        self.dicom.drop_rows_by_tags({"invalid": "bla", "another_invalid_col": "bla"})
        self.assertEqual(self.count, self.dicom.metadata.count())

    def test_drop_invalid_valid_col_mix(self):
        """test drop tags with a mix of valid and invalid tags"""
        # first we will extract a valid tag number and value from the xml
        first_row = self.dicom.metadata.to_pandas()["metadata"][0]
        xml_data = etree.fromstring(first_row.encode("ascii", "ignore"))
        patient_id = xml_data.xpath(self.query.replace("KEYWORD", "PatientID"))[0]
        patient_id_tag = xml_data.xpath(self.element_query.replace("KEYWORD", "PatientID"))[0].get("tag")

        # now we ask dicom to drop by tags giving it both the valid tag-value
        # pair we extracted and also an invalid tag-value pair
        self.dicom.drop_rows_by_tags({patient_id_tag: patient_id, "Invalid": "bla"})

        # since zero records match both criteria dicom should return no records
        self.assertEqual(self.count, self.dicom.metadata.count())

    def test_drop_invalid_type(self):
        """test drop tags with invalid param type"""
        with self.assertRaisesRegexp(Exception, "incomplete format"):
            self.dicom.drop_rows_by_tags(1)

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
                this_row_keyword_value = xml.xpath(self.query.replace("KEYWORD", keyword))[0]
                if this_row_keyword_value != keywords[keyword]:
                    matching_metadata.append(ascii_xml)
                    matching_pixeldata.append(pixeldata)

        return {"metadata": matching_metadata, "pixeldata": matching_pixeldata}

    def _compare_dicom_with_expected_result(self, expected_result):
        """compare expected result with actual result"""
        pandas_metadata = self.dicom.metadata.to_pandas()["metadata"]
        pandas_pixeldata = self.dicom.pixeldata.to_pandas()["imagematrix"]
        for expected, actual in zip(expected_result["metadata"], pandas_metadata):
            actual_ascii = actual.encode("ascii", "ignore")
            self.assertEqual(actual_ascii, expected)
        for expected, actual in zip(expected_result["pixeldata"], pandas_pixeldata):
            numpy.testing.assert_equal(expected, actual)


if __name__ == "__main__":
    unittest.main()
