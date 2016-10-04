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

"""tests import_dicom functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import os
import numpy
import dicom


class CreateDicomTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(CreateDicomTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = self.get_local_dataset("dicom_xml/")
        self.image_directory = self.get_local_dataset("dicom_uncompressed/")

    def test_metadata_content_import_dcm_basic(self):
        """content test of dicom metadata import"""
        # here we will get the files so we can generate the expected result
        files = []
        for filename in sorted([f for f in os.listdir(self.xml_directory)]):
            with open(self.xml_directory + str(filename)) as xmlfile:
                contents = xmlfile.read()
                files.append(contents)

        # the BulkData location element of the metadata xml will be different
        # since the dicom may load the data from a differnet location then
        # where we loaded our files. We will remove this element from the metadata
        # before we compare
        metadata_pandas = self.dicom.metadata.to_pandas()
        for (dcm_file, xml_file) in zip(metadata_pandas["metadata"], files):
            dcm_file = dcm_file.encode("ascii", "ignore")
            bulk_data_index = xml_file.index("<BulkData")
            # extract the bulk data tag from each so we can remove it
            # note that we could do this with lxml etree instead of string
            # ops but since we only care about one tag we don't need to
            xml_bulk_data = xml_file[bulk_data_index:bulk_data_index + xml_file[bulk_data_index:].index(">") + 1]
            dcm_bulk_data = dcm_file[bulk_data_index:bulk_data_index + dcm_file[bulk_data_index:].index(">") + 1]
            xml_file = xml_file.replace(xml_bulk_data, "")
            dcm_file = dcm_file.replace(dcm_bulk_data, "")

            # finally we check that the metadata in dicom
            # matches the dcm file metadata
            self.assertEqual(dcm_file, xml_file)

    @unittest.skip("image content compare fails for some dicom images")
    def test_image_content_import_dcm_basic(self):
        """content test of image data for dicom"""
        # load the files so we can compare with the dicom result
        files = []
        count = 0
        for filename in sorted([f for f in os.listdir(self.image_directory)]):
            pixel_data = dicom.read_file(self.image_directory + filename).pixel_array
            files.append(pixel_data)

        # iterate through the data in the files and in the dicom frame
        # and ensure that they match
        image_pandas = self.dicom.pixeldata.to_pandas()
        for (dcm_image, pixel_image) in zip(image_pandas["imagematrix"], files):
            count = count + 1
            numpy.testing.assert_equal(pixel_image, dcm_image)

    def test_import_dicom_invalid_files(self):
        """tests import dicom with invalid data"""
        dataset = self.get_file("int_str_int.csv")
        with self.assertRaisesRegexp(Exception, "Not a DICOM Stream"):
            dicom = self.context.dicom.import_dcm(dataset)
            dicom.metadata.count()

    def test_import_dicom_mixed_file_types(self):
        """tests import dicom with mixed file types"""
        dataset = self.get_file("dicom/baddicom/")
        dicom = self.context.dicom.import_dcm(dataset)
        with self.assertRaisesRegexp(Exception, "Not a DICOM Stream"):
            dicom.metadata.count()


if __name__ == "__main__":
    unittest.main()
