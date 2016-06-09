##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
"""
   Usage:  python2.7 udf_test.py
   Test UDF implementation.
   ia.udf_dependencies.append(['user.py', '/home/public/test.py', ...])
   ...
   add_columns(user.my_udf, <schema>)

Functionality tested:
  positive tests:
    function in different file
    package name only
    full path
    single file in list
    single file in string (no brackets)
    multiple files
    remote file accesses another
    remote file accesses standard package (e.g. math)
    empty installation list
  negative tests:
    non-existent file
    empty file (no code)
    indirectly called function is missing

  non-tests (cannot catch with assertRaises):
    import & install of a non-existent file is a load-time syntax error.
    import & install of an existing file, with call to non-existent function,
      is a run-time AttributeError
"""

__author__ = "WDW"
__credits__ = ["Prune Wickart", "Anjali Sood"]
__version__ = "2015.01.12"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

import qalib
from qalib.udftestlib import udf_remote_utils_direct
from qalib import frame_utils, common_utils
from qalib import atk_test
from qalib.udftestlib import udf_remote_utils_select


class UDFTest(atk_test.ATKTestCase):
    _multiprocess_shared_ = False
    
    @classmethod
    def setUpClass(cls):
        super(UDFTest, cls).setUpClass()
        qalib_path = qalib.__path__[0]
        sys.path.append(qalib.__path__)
        pwd = os.path.dirname(os.path.realpath(__file__))
        remote_path = udf_remote_utils_direct.__file__
        remote_prefix = remote_path[0:remote_path.rfind('/')]
        install_prefix = remote_prefix + '/'
        install_prefix = "../qalib/udftestlib"
        # remove final /
        # self.qalib = remote_prefix[0:remote_path.rfind('/')-1]
        # self.qalib = [self.auto_tests[0:self.auto_tests.rfind('/')]]
        print "Test path:", pwd
        print "UDFs path:", remote_path
        print "Look path:", install_prefix
        print "auto path:", qalib_path
        ia.udf_dependencies = [qalib_path]

    def setUp(self):
        super(UDFTest, self).setUp()


        self.data_basic = "int_str_int.csv"
        self.schema_basic = [("num1", ia.int32),
                             ("letter", str),
                             ("num2", ia.int32)]

    def test_udf_basic_module_install(self):
        """
        First test case from UDF testing;
          this one will likely appear in the user documentation,
          albeit with a different or unspecified input file and schema.
        This depends on adjacent files udf_remote_utils1 and udf_remote_utils2.
        :return:
        """
        # positive tests:udf_remote_utils_direct
        #   Install one module (list with one string)


        frame = frame_utils.build_frame(
            self.data_basic, self.schema_basic, self.prefix)
        print frame.inspect()

        # Try both absolute and relative paths.
        #     [self.install_prefix + 'udf_remote_utils_indirect.py',
        #      self.install_prefix + 'udf_remote_utils_direct.py']
        # )

        # Test as lambda;
        #   "new" column is num2+1
        frame.add_columns(
            lambda row:
            udf_remote_utils_direct.length(row.letter) + row.num2,
            ('new_column', ia.int32))
        print frame.inspect()
        from subprocess import call
        call(["touch","../qalib/test.txt"])

        # Test as explicit call;
        #   "other" column is len(letter), which is 1 in all 3 rows.
        frame.add_columns(udf_remote_utils_direct.row_build,
                          ('other_column', ia.int32))
        print frame.inspect()
        frame_take = frame.take(frame.row_count)
        letter = [x[1] for x in frame_take]
        a_row = letter.index('a')
        c_row = letter.index('c')
        self.assertEqual(frame_take[c_row][-1], len(frame_take[c_row][1]))
        self.assertEqual(frame_take[a_row][3], frame_take[a_row][2]+1)

    def test_udf_basic(self):
        """
        First test case from UDF testing;
          this one will likely appear in the user documentation,
          albeit with a different or unspecified input file and schema.
        This depends on adjacent files udf_remote_utils1 and udf_remote_utils2.
        :return:
        """
        # positive tests:udf_remote_utils_direct
        #   Install one module simple string


        frame = frame_utils.build_frame(
            self.data_basic, self.schema_basic, self.prefix)
        print frame.inspect()

        # Try both absolute and relative paths.
        # ia.udf_dependencies.append(
        #     [self.install_prefix + 'udf_remote_utils_indirect.py',
        #      self.install_prefix + 'udf_remote_utils_direct.py']
        # )

        # Test as lambda;
        #   "new" column is num2+1
        frame.add_columns(
            lambda row: udf_remote_utils_direct.length(row.letter) + row.num2,
            ('new_column', ia.int32))
        print frame.inspect()

        # Test as explicit call;
        #   "other" column is len(letter), which is 1 in all 3 rows.
        frame.add_columns(udf_remote_utils_direct.row_build,
                          ('other_column', ia.int32))
        print frame.inspect()
        frame_take = frame.take(frame.row_count)
        letter = [x[1] for x in frame_take]
        a_row = letter.index('a')
        c_row = letter.index('c')
        self.assertEqual(frame_take[c_row][-1], len(frame_take[c_row][1]))
        self.assertEqual(frame_take[a_row][3], frame_take[a_row][2]+1)

    def test_udf_indirect_std(self):
        """
        First test case from UDF testing;
          This will likely appear in the user documentation,
          albeit with a different or unspecified input file and schema.
        This depends on adjacent files udf_remote_utils1 and udf_remote_utils2.
        :return:
        """
        # single file in list
        # remote file accesses standard package (e.g. math)

        from qalib.udftestlib import udf_remote_utils_indirect

        sqrt5 = 2.2360679775

        frame = frame_utils.build_frame(
            self.data_basic, self.schema_basic, self.prefix)
        print frame.inspect()

        # Test as explicit call;
        # Each row has num1:num2 in a 1:2 ratio,
        #   so each "other" entry is num1 * sqrt(5)
        frame.add_columns(udf_remote_utils_indirect.distance,
                          ('other_column', ia.float64))
        print frame.inspect()
        frame_take = frame.take(frame.row_count)
        letter = [x[1] for x in frame_take]
        a_row = letter.index('a')
        c_row = letter.index('c')
        self.assertAlmostEqual(
            frame_take[c_row][-1], frame_take[c_row][0] * sqrt5)
        self.assertAlmostEqual(
            frame_take[a_row][-1], frame_take[a_row][0] * sqrt5)

    def test_udf_indirect_delayed(self):
        """
        Call a remote function.
        That function is responsible to locate another remote function.
        Each context has a call to ia.udf_dependencies.append.
        :return:
        """

        # ia.udf_dependencies.append(['udf_remote_utils_direct.py',
        #                 'udf_remote_utils_indirect.py'])

        frame = frame_utils.build_frame(
            self.data_basic, self.schema_basic, self.prefix)
        print frame.inspect()

        # This function grabs num2 for letter 'b', num1 for all others.
        udf_remote_utils_select.add_select_col(frame)
        print frame.inspect()
        frame_take = frame.take(frame.row_count)
        letter = [x[1] for x in frame_take]
        a_row = letter.index('a')
        b_row = letter.index('b')

        # Selector should give different grabs for first two rows
        self.assertEqual(frame_take[a_row][-1], frame_take[a_row][0])
        self.assertEqual(frame_take[b_row][-1], frame_take[b_row][2])

    def test_udf_indirect_missing(self):
        """
        Use a function that is missing a support function (indirect call).
        :return:
        """
        # positive tests:
        #   single file in string (no brackets)
        # negative tests:
        #   indirectly called function is missing

        # import qalib.udftestlib.udf_remote_utils_empty   # import empty file # noqa

        # Install an empty file; must not interfere with useful file.
        # ia.udf_dependencies.append([self.install_prefix + 'udf_remote_utils_direct.py',
        #                 self.install_prefix + 'udf_remote_utils_empty.py'])

        frame = frame_utils.build_frame(
            self.data_basic, self.schema_basic, self.prefix)
        print frame.inspect()

        # Test as lambda
        self.assertRaises(
            Exception, frame.add_columns,
            lambda row: bogus_dependency.not_exist(row.letter) + row.num2,
            ('new_column', ia.int32)
        )


if __name__ == "__main__":
    unittest.main()
