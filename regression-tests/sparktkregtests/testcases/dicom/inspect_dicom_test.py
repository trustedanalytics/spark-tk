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
from lxml import etree


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

    def test_metadata_content_inspect_dcm_basic(self):
        """content test of dicom metadata import"""
        # here we will get the files so we can generate the expected result
        files = []
        for filename in os.listdir(self.xml_directory):
            if filename.endswith(".xml"):
                with open(self.xml_directory + str(filename), 'rb') as xmlfile:
                    contents = xmlfile.read()
                    xml = etree.fromstring(contents)
                    bulk_data = xml.xpath("//BulkData")[0]
                    bulk_data.getparent().remove(bulk_data)
                    files.append(etree.tostring(xml))

        # the BulkData location element of the metadata xml will be different
        # since the dicom may load the data from a differnet location then
        # where we loaded our files. We will remove this element from the metadata
        # before we compare
        metadata_inspect = self.dicom.metadata.inspect().rows
        dicom_metadata = []
        for dcm_file in metadata_inspect:
            dcm_file = dcm_file[1].encode("ascii", "ignore")
            dcm_xml_root = etree.fromstring(dcm_file)
            dcm_bulk_data = dcm_xml_root.xpath("//BulkData")[0]
            dcm_bulk_data.getparent().remove(dcm_bulk_data)
            dicom_metadata.append(etree.tostring(dcm_xml_root))
        
        for metadata in dicom_metadata:
            result = metadata in files
            self.assertTrue(result)

    def test_image_content_inspect_dcm_basic(self):
        """content test of image data for dicom"""
        # load the files so we can compare with the dicom result
        files = []
        for filename in sorted([f for f in os.listdir(self.image_directory)]):
            pixel_data = dicom.read_file(self.image_directory + filename).pixel_array
            files.append(pixel_data)

        # iterate through the data in the files and in the dicom frame
        # and ensure that they match
        image_inspect = self.dicom.pixeldata.inspect()
        for (dcm_image, pixel_image) in zip(image_inspect.rows, files):
            numpy.testing.assert_equal(pixel_image, dcm_image[1])


if __name__ == "__main__":
    unittest.main()
