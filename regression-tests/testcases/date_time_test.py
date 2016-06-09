##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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
    Usage:  python2.7 date_time_test.py
    Test datetime data type.
"""
__author__ = 'Karen Herrold'
__credits__ = ["Karen Herrold"]
__version__ = "2015.09.14"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import datetime

import trustedanalytics as atk

from qalib import frame_utils
from qalib import atk_test


class DateTimeTest(atk_test.ATKTestCase):

    def setUp(self):
        super(DateTimeTest, self).setUp()

        # Create table containing datetime data in multiple formats.
        datafile = "dates.csv"

        # Define schema.
        schema_data = [("day", atk.datetime),
                       ("month", atk.datetime),
                       ("day_time_offset", atk.datetime),
                       ("day_time_zulu", atk.datetime),
                       ("week", atk.datetime),
                       ("week_day", atk.datetime),
                       ("day_of_year", atk.datetime),
                       ("time", atk.datetime),
                       ("time_zulu", atk.datetime)]

        # Create ATK frame.
        self.frame = frame_utils.build_frame(
            datafile, schema_data, prefix=self.prefix)

    def test_date_time(self):
        """
        Happy path test for trusted analytics datetime data type.
        """
        def make_date_time(row):
            d = row.day.date()                   # In local time zone
            t = row.time.timetz()                # Contains time zone desired
            dt = datetime.datetime.combine(d, t)
            eqflg = (dt == row.day_time_offset)  # Compare with expected value
            doy = row.day_of_year.date()         # In local time zone
            t_z = row.time_zulu.timetz()         # Contains time zone desired
            dtz = datetime.datetime.combine(doy, t_z)
            eqflg2 = (dtz == row.day_time_zulu)  # Compare with expected value
            return [dt, unicode(eqflg), dtz, unicode(eqflg2)]

        self.frame.add_columns(make_date_time,
                               [('new_date', atk.datetime),
                                ('eqflg', unicode),
                                ('new_date2', atk.datetime),
                                ('eqflg2', unicode)],
                               columns_accessed=['day',
                                                 'time',
                                                 'day_time_offset',
                                                 'day_of_year',
                                                 'time_zulu',
                                                 'day_time_zulu'])
        flags = self.frame.take(self.frame.row_count,
                                columns=['eqflg', 'eqflg2'])

        expected = [[u'True', u'True'],
                    [u'True', u'True'],
                    [u'True', u'True']]

        self.assertEqual(expected,
                         flags,
                         msg="Actual results did not match expected results."
                             "\nExpected:\n{0}\n\nActual:\n{1}\n\nData:\n{2}"
                             .format(expected,
                                     flags,
                                     self.frame.inspect(self.frame.row_count)))

if __name__ == "__main__":
    unittest.main()
