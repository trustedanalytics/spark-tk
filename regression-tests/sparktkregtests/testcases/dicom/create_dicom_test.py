"""tests import_dicom functionality"""

import unittest
from sparktkregtests.lib import sparktk_test
import os
import dicom
import numpy


class CreateDicomTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(CreateDicomTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = "../../../datasets/dicom/dicom_uncompressed/xml/"
        self.image_directory = "../../../datasets/dicom/dicom_uncompressed/imagedata/"

    def test_metadata_content_import_dcm_basic(self):
        files = []
        for filename in os.listdir(self.xml_directory):
            with open(self.xml_directory + str(filename)) as xmlfile:
                contents = xmlfile.read()
                files.append(contents)
        metadata_pandas = self.dicom.metadata.to_pandas()
        for (dcm_file, xml_file) in zip(metadata_pandas["metadata"], files):
            dcm_file = dcm_file.encode("ascii", "ignore")
            bulk_data_index = xml_file.index("<BulkData")
            xml_bulk_data = xml_file[bulk_data_index:bulk_data_index + xml_file[bulk_data_index:].index(">") + 1]
            dcm_bulk_data = dcm_file[bulk_data_index:bulk_data_index + dcm_file[bulk_data_index:].index(">") + 1]

            xml_file = xml_file.replace(xml_bulk_data, "")
            dcm_file = dcm_file.replace(dcm_bulk_data, "")

            self.assertEqual(dcm_file, xml_file)
            
    def test_image_content_import_dcm_basic(self):
        files = []
        for filename in os.listdir(self.image_directory):
            pixel_data = dicom.read_file(self.image_directory + filename).pixel_array
            files.append(pixel_data)

        image_pandas = self.dicom.pixeldata.to_pandas()
        for (dcm_image, pixel_image) in zip(image_pandas["imagematrix"], files):
            numpy.testing.assert_equal(pixel_image, dcm_image)

    def test_import_dicom_invalid_files(self):
        dataset = self.get_file("int_str_int.csv")
        with self.assertRaisesRegexp(Exception, "Not a DICOM Stream"):
            dicom = self.context.dicom.import_dcm(dataset)
            dicom.metadata.count()

    def test_import_dicom_mixed_file_types(self):
        dataset = self.get_file("dicom/baddicom/")
        dicom = self.context.dicom.import_dcm(dataset)
        with self.assertRaisesRegexp(Exception, "Not a DICOM Stream"):
            dicom.metadata.count()


if __name__ == "__main__":
    unittest.main()
