"""tests dicom.inspect() functionality"""

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
        self.count = self.dicom.metadata.count()

    def test_metadata_imagedata_row_count_same(self):
        metadata_result = self.dicom.metadata.inspect(self.dicom.metadata.count())
        image_result = self.dicom.pixeldata.inspect(self.dicom.pixeldata.count())
        self.assertEqual(len(metadata_result.rows), len(image_result.rows))

    def test_metadata_content(self):
        files = []
        for filename in os.listdir(self.xml_directory):
            with open(self.xml_directory + str(filename)) as xmlfile:
                contents = xmlfile.read()
                files.append(contents)

        take = self.dicom.metadata.take(self.count)

        for (dcm_file, xml_file) in zip(take.data, files):
            dcm_file = dcm_file[1].encode("ascii", "ignore")
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

        take = self.dicom.pixeldata.take(self.count)
        for (dcm_image, pixel_image) in zip(take.data, files):
            numpy.testing.assert_equal(pixel_image, dcm_image[1])


if __name__ == "__main__":
    unittest.main()
