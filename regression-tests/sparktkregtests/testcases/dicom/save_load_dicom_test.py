"""tests dicom.save and dicom.load functionality"""

import unittest
from unittest import TestCase
from sparktkregtests.lib import sparktk_test
import os
import dicom
import numpy
import datetime


class SaveLoadDicomTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(SaveLoadDicomTest, self).setUp()
        self.dataset = self.get_file("dicom_uncompressed")
        self.dicom = self.context.dicom.import_dcm(self.dataset)
        self.xml_directory = "../../../datasets/dicom/dicom_uncompressed/xml/"
        self.image_directory = "../../../datasets/dicom/dicom_uncompressed/imagedata/"
        # generate a unique name to save the dicom object under
        self.location = "save_load_test" + str(datetime.datetime.now()).replace(":", "-")

    def test_basic_save_load_content_test(self):
        self.dicom.save(self.location)
        load_dicom = self.context.load(self.location)
        original_metadata = self.dicom.metadata.download()["metadata"]
        load_metadata = load_dicom.metadata.download()["metadata"]
        
        for (load_row, original_row) in zip(original_metadata, load_metadata):
            original_file = original_row.encode("ascii", "ignore")
            loaded_file = load_row.encode("ascii", "ignore")
            bulk_data_index = original_file.index("<BulkData")
            load_bulk_data = loaded_file[bulk_data_index:bulk_data_index + loaded_file[bulk_data_index:].index(">") + 1]
            original_bulk_data = original_file[bulk_data_index:bulk_data_index + original_file[bulk_data_index:].index(">") + 1]                                                                                                                        
            loaded_file = loaded_file.replace(load_bulk_data, "")
            original_file = original_file.replace(original_bulk_data, "")
  
            self.assertEqual(loaded_file, original_file)
 
        original_image = self.dicom.pixeldata.download()
        loaded_image = load_dicom.pixeldata.download()
        for (dcm_image, pixel_image) in zip(original_image["imagematrix"], loaded_image["imagematrix"]):
            numpy.testing.assert_equal(pixel_image, dcm_image)

    def test_save_invalid_long_unicode_name(self):
        metadata_unicode = self.dicom.metadata.download()["metadata"][0]
        with self.assertRaisesRegexp(Exception, "IllegalArgumentException"):
            self.dicom.save(metadata_unicode)

    def test_load_does_not_exist(self):
        with self.assertRaisesRegexp(Exception, "Input path does not exist"):
            self.context.load("does_not_exist")

    def test_save_invalid_path_type(self):
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.dicom.save(1)

    def test_save_name_already_exists(self):
        with self.assertRaisesRegexp(Exception, "already exists"):
            self.dicom.save("duplicate_name")
            self.dicom.save("duplicate_name")


if __name__ == "__main__":
    unittest.main()
