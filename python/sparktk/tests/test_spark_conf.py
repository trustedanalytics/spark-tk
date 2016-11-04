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

import unittest
import os

from sparktk.sparkconf import _parse_spark_conf


class TestParseSparkConf(unittest.TestCase):

    def test_parse_spark_conf_good(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        conf = _parse_spark_conf(os.path.join(dir_path, 'spark-pass.conf'))

        self.assertEqual(16, len(conf))
        self.assertEqual('false', conf['spark.shuffle.io.preferDirectBufs'])
        self.assertEqual('2', conf['spark.executor.cores'])
        self.assertEqual('-Xmx1536m', conf['spark.executor.extrajavaoptions'])
        self.assertEqual('2g', conf['spark.driver.maxResultSize'])
        self.assertEqual('384', conf['spark.yarn.executor.memoryOverhead'])

    def test_parse_spark_conf_bad(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        try:
            _parse_spark_conf(os.path.join(dir_path, 'spark-fail.conf'))
        except RuntimeError as e:
            self.assertTrue("may be missing an '='" in str(e))
        else:
            self.fail("Expected spark conf parser to fail on bad conf")


if __name__ == '__main__':
    unittest.main()
