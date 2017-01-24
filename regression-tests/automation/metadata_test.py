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


""" Test accuracy of metadata file """

import unittest
import os
from sparktkregtests.lib import sparktk_test
import sparktkregtests.lib.config as config


class MetadataTest(sparktk_test.SparkTKTestCase):

    def test_metadata_has_no_duplicates(self):
        """Test metadata file does not have duplicates"""
        meta_list = []
        filename = self.get_local_dataset('metadata.txt')
        with open(filename, 'r') as f:
            for line in f:
                alpha = line.split(',', 2)
                meta_list.append(alpha[0])
        datasets = []
        for file in os.listdir(config.dataset_directory):
            datasets.append(file)
        datasets.remove('metadata.txt')
        duplicates = len(meta_list) - len(set(meta_list))
        is_not_documented = list(set(datasets) - set(meta_list))
        does_not_exist = list(set(meta_list) - set(datasets))

        self.assertEqual(duplicates, 0)
        self.assertEqual(is_not_documented, [])
        self.assertListEqual(does_not_exist, [])


if __name__ == '__main__':
    unittest.main()
