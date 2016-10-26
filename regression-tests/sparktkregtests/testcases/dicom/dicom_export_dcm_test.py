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
import numpy
from lxml import etree


class ExportDicomTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(ExportDicomTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = self.get_local_dataset("dicom_xml/")
        self.image_directory = self.get_local_dataset("dicom_uncompressed/")

    # @unittest.skip("Sparktk: dicom export to dcm throws error")
    def test_export_to_dcm(self):
        # first we will set aside the original data so that we can use it to
        # compare with later
        original_metadata = self.dicom.metadata.to_pandas()["metadata"]
        original_formatted = []
        # we will remove the bulkdata tag from our original metadata
        # since the bulkdata tag records source of dicom and should
        # be ignored during comparison
        for metadata in original_metadata:
            ascii_metadata = metadata.encode("ascii", "ignore")
            xml_root = etree.fromstring(ascii_metadata)
            bulk_data_tag = xml_root.xpath("//BulkData")[0]
            bulk_data_tag.getparent().remove(bulk_data_tag)
            original_formatted.append(etree.tostring(xml_root))
        original_imagedata = self.dicom.pixeldata.to_pandas()["imagematrix"]

        # now we export the dicom object
        # we use our QA libraries to generate a path
        # we save the dicom to that path
        dcm_export_path = self.get_export_file(self.get_name("DICOM_EXPORT"))
        self.dicom.export_to_dcm(dcm_export_path)

        # Now we will load back the data we just saved into a new dicom object
        # so that we can ensure the data is the same
        loaded_dicom = self.context.dicom.import_dcm(dcm_export_path)

        # get the loaded metadata and imagedata
        loaded_metadata = loaded_dicom.metadata.to_pandas()["metadata"]
        loaded_imagedata = loaded_dicom.pixeldata.to_pandas()["imagematrix"]

        # ensure that the loaded metadata and imagedata is of the same len
        # as the original data,
        # then iterate through the records and ensure they are the same
        self.assertEqual(len(loaded_metadata), len(original_metadata))
        self.assertEqual(len(loaded_imagedata), len(original_imagedata))
        for actual in loaded_metadata:
            # for each metadata we will remove the bulkdata tag before we
            # compare it with the original data since the bulk data tag
            # records the source for the dcm and may differ since
            # we are loading from a different location than the original dicom
            actual_root = etree.fromstring(actual.encode("ascii", "ignore"))
            actual_bulk_data_tag = actual_root.xpath("//BulkData")[0]
            actual_bulk_data_tag.getparent().remove(actual_bulk_data_tag)
            actual = etree.tostring(actual_root)
            self.assertTrue(actual in original_formatted)
        for actual in loaded_imagedata:
            result = any(numpy.array_equal(actual, original) for original in original_imagedata)
            self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
