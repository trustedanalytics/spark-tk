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

"""tests export_dcm functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import os
import numpy
import dicom
from lxml import etree


class ExportDicomTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(ExportDicomTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = self.get_local_dataset("dicom_xml/")
        self.image_directory = self.get_local_dataset("dicom_uncompressed/")

    @unittest.skip("Sparktk: dicom export to dcm throws error")
    def test_export_to_dcm(self):
        original_metadata = self.dicom.metadata.to_pandas()["metadata"]
        original_imagedata = self.dicom.pixeldata.to_pandas()["imagematrix"]
        dcm_export_path = self.get_export_file(self.get_name("DICOM_EXPORT"))
        self.dicom.export_to_dcm(dcm_export_path)
        loaded_dicom = self.context.dicom.import_dcm(dm_export_path)

        loaded_metadata = loaded_dicom.metadata.to_pandas()["metadata"]
        loaded_imagedata = loaded_dicom.pixeldata.to_pandas()["imagematrix"]

        self.assertEqual(len(loaded_metadata), len(original_metadata))
        self.assertEqual(len(loaded_imagedata), len(original_imagedata))
        for (original, actual) in zip(original_metadata, loaded_metadata):
            self.assertEqual(original, actual)
        for (original, actual) in zip(original_imagedata, loaded_imagedata):
            numpy.testing.assert_equal(original, actual)


if __name__ == "__main__":
    unittest.main()
