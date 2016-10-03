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
        # generate a unique name to save the dicom object under
        self.location = "save_load_test" + str(str(datetime.datetime.now()).replace(":", "")).replace(" ", "")

    def test_basic_save_load_content_test(self):
        """basic save load content test"""
        # save the current dicom object under a unique name
        self.dicom.save(self.location)
        # load the dicom
        load_dicom = self.context.load(self.location)
        original_metadata = self.dicom.metadata.to_pandas()["metadata"]
        load_metadata = load_dicom.metadata.to_pandas()["metadata"]
        
        # compare the loaded dicom object with the dicom object we created
        for (load_row, original_row) in zip(original_metadata, load_metadata):
            original_file = original_row.encode("ascii", "ignore")
            
            # extract and remove bulk data element from metadata since we don't care about it
            # bulk data records the file's location, so it may differ
            loaded_file = load_row.encode("ascii", "ignore")
            bulk_data_index = original_file.index("<BulkData")
            load_bulk_data = loaded_file[bulk_data_index:bulk_data_index + loaded_file[bulk_data_index:].index(">") + 1]
            original_bulk_data = original_file[bulk_data_index:bulk_data_index + original_file[bulk_data_index:].index(">") + 1]                                                                                                                        
            loaded_file = loaded_file.replace(load_bulk_data, "")
            original_file = original_file.replace(original_bulk_data, "")
  
            self.assertEqual(loaded_file, original_file)
 
        # now we check that the pixel data matches
        original_image = self.dicom.pixeldata.to_pandas()
        loaded_image = load_dicom.pixeldata.to_pandas()
        for (dcm_image, pixel_image) in zip(original_image["imagematrix"], loaded_image["imagematrix"]):
            numpy.testing.assert_equal(pixel_image, dcm_image)

    def test_save_invalid_long_unicode_name(self):
        """save under a long unicode name, should fail"""
        # we will pass the dicom metadata itself as the name
        metadata_unicode = self.dicom.metadata.to_pandas()["metadata"]
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.dicom.save(metadata_unicode)

    def test_load_does_not_exist(self):
        """test load dicom does not exist"""
        with self.assertRaisesRegexp(Exception, "Input path does not exist"):
            self.context.load("does_not_exist")

    def test_save_invalid_path_type(self):
        """test save dicom invalid path type"""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.dicom.save(1)

    def test_save_name_already_exists(self):
        """test save dicom duplicate name"""
        with self.assertRaisesRegexp(Exception, "already exists"):
            self.dicom.save("duplicate_name")
            self.dicom.save("duplicate_name")


if __name__ == "__main__":
    unittest.main()
