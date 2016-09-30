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

"""tests dicom.inspect() functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import os
import dicom
import numpy


class InspectDicomTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(InspectDicomTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = self.get_local_dataset("dicom_xml/")
        self.image_directory = self.get_local_dataset("dicom_uncompressed/")

    def test_metadata_imagedata_row_count_same(self):
        """test that the row count are the same for inspect pixeldate/metadata"""
        metadata_result = self.dicom.metadata.inspect(self.dicom.metadata.count())
        image_result = self.dicom.pixeldata.inspect(self.dicom.pixeldata.count())
        self.assertEqual(len(metadata_result.rows), len(image_result.rows))

    def test_metadata_content(self):
        """tests metadata inspect content"""
        # first we will get the files we created the dicom from
        files = []
        for filename in os.listdir(self.xml_directory):
            with open(self.xml_directory + str(filename)) as xmlfile:
                contents = xmlfile.read()
                files.append(contents)

        inspect = self.dicom.metadata.inspect()

        # we ensure the metadata in dicom matches the generated
        # xmls from the files we created the dicom from
        for (dcm_file, xml_file) in zip(inspect.rows, files):
            # we need to remove the bulkdata tag before we compare since
            # it records the location where the files were loaded from
            # and therefore will differ between the content
            dcm_file = dcm_file[1].encode("ascii", "ignore")
            bulk_data_index = xml_file.index("<BulkData")
            xml_bulk_data = xml_file[bulk_data_index:bulk_data_index + xml_file[bulk_data_index:].index(">") + 1]
            dcm_bulk_data = dcm_file[bulk_data_index:bulk_data_index + dcm_file[bulk_data_index:].index(">") + 1]

            xml_file = xml_file.replace(xml_bulk_data, "")
            dcm_file = dcm_file.replace(dcm_bulk_data, "")

            self.assertEqual(dcm_file, xml_file)

    def test_image_content_inspect_dcm_basic(self):
        """image content test for dicom inspect"""
        # first we get the files from the dataset directory
        files = []
        for filename in os.listdir(self.image_directory):
            pixel_data = dicom.read_file(self.image_directory + filename).pixel_array
            files.append(pixel_data)

        # then we compare the image data with dicom's inspect result
        inspect = self.dicom.pixeldata.inspect()
        for (dcm_image, pixel_image) in zip(inspect.rows, files):
            numpy.testing.assert_equal(pixel_image, dcm_image[1])


if __name__ == "__main__":
    unittest.main()
