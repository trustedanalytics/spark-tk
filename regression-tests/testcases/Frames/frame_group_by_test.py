#############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014-2016 Intel Corporation All Rights Reserved.
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
   Usage:  python2.7 frame_group_by_test.py
   Test functionality of group_by, including aggregation_arguments
"""

__author__ = "KH, WDW"
__credits__ = ["Grayson Churchel", "Karen Herrold", "Prune Wickart"]
__version__ = "2016.02.18"

# Functionality tested:
#   frame.group_by(group_by_columns, aggregation_arguments)
#   - avg
#   - count
#   - count_distinct
#   - max
#   - min
#   - stdev
#   - sum
#   - var
#
#   UDF aggregation function
#   - all data types for cols
#   - 0, 1, and multiple cols
#   - missing and surplus initial values
#   - diff length of schema and initial value vectors
#
#   error tests:
#   - group_by column does not exist
#   - group_by aggregation column does not exist
#   - group_by aggregation count_distinct where column contains 'None'
#   - group_by aggregation statistical functions where column contains strings
#   - invalid aggregation attribute
#
# This test suite replaces the following manual tests:
#   - Aggregations01.py
#   - Aggregations01B.py
#   - Aggregations02.py
#   - AnimalCounts.py
#   - MultiAggregations01.py

# TODO
# This test suite also tests import errors which probably should be moved
# to a test suite focusing on import.

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ta
import numpy as np

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test
from itertools import count
from operator import itemgetter


class GroupByTest(atk_test.ATKTestCase):

    expected_rows = 128

    # group by single column, aggregate on column for data type tested
    # group_by, avg, count, count_distinct, max, min, stdev, sum, var
    results1 = \
        [0, 8.0, 8, 2, 16, 0, 8.55235974119758, 64, 73.14285714285714,
         1, 9.0, 8, 2, 17, 1, 8.55235974119758, 72, 73.14285714285714,
         2, 10.0, 8, 2, 18, 2, 8.55235974119758, 80, 73.14285714285714,
         3, 11.0, 8, 2, 19, 3, 8.55235974119758, 88, 73.14285714285714,
         4, 12.0, 8, 2, 20, 4, 8.55235974119758, 96, 73.14285714285714,
         5, 13.0, 8, 2, 21, 5, 8.55235974119758, 104, 73.14285714285714,
         6, 14.0, 8, 2, 22, 6, 8.55235974119758, 112, 73.14285714285714,
         7, 15.0, 8, 2, 23, 7, 8.55235974119758, 120, 73.14285714285714,
         8, 16.0, 8, 2, 24, 8, 8.55235974119758, 128, 73.14285714285714,
         9, 17.0, 8, 2, 25, 9, 8.55235974119758, 136, 73.14285714285714,
         10, 18.0, 8, 2, 26, 10, 8.55235974119758, 144, 73.14285714285714,
         11, 19.0, 8, 2, 27, 11, 8.55235974119758, 152, 73.14285714285714,
         12, 20.0, 8, 2, 28, 12, 8.55235974119758, 160, 73.14285714285714,
         13, 21.0, 8, 2, 29, 13, 8.55235974119758, 168, 73.14285714285714,
         14, 22.0, 8, 2, 30, 14, 8.55235974119758, 176, 73.14285714285714,
         15, 23.0, 8, 2, 31, 15, 8.55235974119758, 184, 73.14285714285714]

    # group by two columns, aggregate on column for data type tested
    # group_by, avg, count, count_distinct, max, min, stdev, sum, var
    results2 = \
        [0, 0, 0.0, 4, 1, 0, 0, 0.0, 0, 0.0,
         1, 1, 1.0, 4, 1, 1, 1, 0.0, 4, 0.0,
         2, 2, 2.0, 4, 1, 2, 2, 0.0, 8, 0.0,
         3, 3, 3.0, 4, 1, 3, 3, 0.0, 12, 0.0,
         4, 4, 4.0, 4, 1, 4, 4, 0.0, 16, 0.0,
         5, 5, 5.0, 4, 1, 5, 5, 0.0, 20, 0.0,
         6, 6, 6.0, 4, 1, 6, 6, 0.0, 24, 0.0,
         7, 7, 7.0, 4, 1, 7, 7, 0.0, 28, 0.0,
         8, 8, 8.0, 4, 1, 8, 8, 0.0, 32, 0.0,
         9, 9, 9.0, 4, 1, 9, 9, 0.0, 36, 0.0,
         10, 10, 10.0, 4, 1, 10, 10, 0.0, 40, 0.0,
         11, 11, 11.0, 4, 1, 11, 11, 0.0, 44, 0.0,
         12, 12, 12.0, 4, 1, 12, 12, 0.0, 48, 0.0,
         13, 13, 13.0, 4, 1, 13, 13, 0.0, 52, 0.0,
         14, 14, 14.0, 4, 1, 14, 14, 0.0, 56, 0.0,
         15, 15, 15.0, 4, 1, 15, 15, 0.0, 60, 0.0,
         16, 0, 16.0, 4, 1, 16, 16, 0.0, 64, 0.0,
         17, 1, 17.0, 4, 1, 17, 17, 0.0, 68, 0.0,
         18, 2, 18.0, 4, 1, 18, 18, 0.0, 72, 0.0,
         19, 3, 19.0, 4, 1, 19, 19, 0.0, 76, 0.0,
         20, 4, 20.0, 4, 1, 20, 20, 0.0, 80, 0.0,
         21, 5, 21.0, 4, 1, 21, 21, 0.0, 84, 0.0,
         22, 6, 22.0, 4, 1, 22, 22, 0.0, 88, 0.0,
         23, 7, 23.0, 4, 1, 23, 23, 0.0, 92, 0.0,
         24, 8, 24.0, 4, 1, 24, 24, 0.0, 96, 0.0,
         25, 9, 25.0, 4, 1, 25, 25, 0.0, 100, 0.0,
         26, 10, 26.0, 4, 1, 26, 26, 0.0, 104, 0.0,
         27, 11, 27.0, 4, 1, 27, 27, 0.0, 108, 0.0,
         28, 12, 28.0, 4, 1, 28, 28, 0.0, 112, 0.0,
         29, 13, 29.0, 4, 1, 29, 29, 0.0, 116, 0.0,
         30, 14, 30.0, 4, 1, 30, 30, 0.0, 120, 0.0,
         31, 15, 31.0, 4, 1, 31, 31, 0.0, 124, 0.0]

    # group by None (no grouping), aggregate on column for data type tested
    # avg, count, count_distinct, max, min, stdev, sum, var
    results3 = \
        [15.5, 128, 32, 31, 0, 9.269372138528029, 1984, 85.92125984251969]

    # group by single column, aggregate on column containing 'None'.
    # group_by, avg, count, count_distinct, max, min, stdev, sum, var
    results4 = [0, None, 8, 1, None, None, None, 0, None,
                1, 1.0, 8, 1, 1, 1, 0.0, 8, 0.0,
                2, 2.0, 8, 1, 2, 2, 0.0, 16, 0.0,
                3, 3.0, 8, 1, 3, 3, 0.0, 24, 0.0,
                4, 4.0, 8, 1, 4, 4, 0.0, 32, 0.0,
                5, None, 8, 1, None, None, None, 0, None,
                6, 6.0, 8, 1, 6, 6, 0.0, 48, 0.0,
                7, 7.0, 8, 1, 7, 7, 0.0, 56, 0.0,
                8, 8.0, 8, 1, 8, 8, 0.0, 64, 0.0,
                9, None, 8, 1, None, None, None, 0, None,
                10, None, 8, 1, None, None, None, 0, None,
                11, 11.0, 8, 1, 11, 11, 0.0, 88, 0.0,
                12, 12.0, 8, 1, 12, 12, 0.0, 96, 0.0,
                13, None, 8, 1, None, None, None, 0, None,
                14, 14.0, 8, 1, 14, 14, 0.0, 112, 0.0,
                15, None, 8, 1, None, None, None, 0, None]

    # group by two columns, aggregate on column containing 'None'.
    # groupby1, groupby2, avg, count, count_distinct, max, min, stdev, sum, var
    results5 = [0, 0, None, 4, 1, None, None, None, 0, None,
                1, 1, 1.0, 4, 1, 1, 1, 0.0, 4, 0.0,
                2, 2, 2.0, 4, 1, 2, 2, 0.0, 8, 0.0,
                3, 3, 3.0, 4, 1, 3, 3, 0.0, 12, 0.0,
                4, 4, 4.0, 4, 1, 4, 4, 0.0, 16, 0.0,
                5, 5, None, 4, 1, None, None, None, 0, None,
                6, 6, 6.0, 4, 1, 6, 6, 0.0, 24, 0.0,
                7, 7, 7.0, 4, 1, 7, 7, 0.0, 28, 0.0,
                8, 8, 8.0, 4, 1, 8, 8, 0.0, 32, 0.0,
                9, 9, None, 4, 1, None, None, None, 0, None,
                10, 10, None, 4, 1, None, None, None, 0, None,
                11, 11, 11.0, 4, 1, 11, 11, 0.0, 44, 0.0,
                12, 12, 12.0, 4, 1, 12, 12, 0.0, 48, 0.0,
                13, 13, None, 4, 1, None, None, None, 0, None,
                14, 14, 14.0, 4, 1, 14, 14, 0.0, 56, 0.0,
                15, 15, None, 4, 1, None, None, None, 0, None,
                16, 0, None, 4, 1, None, None, None, 0, None,
                17, 1, 1.0, 4, 1, 1, 1, 0.0, 4, 0.0,
                18, 2, 2.0, 4, 1, 2, 2, 0.0, 8, 0.0,
                19, 3, 3.0, 4, 1, 3, 3, 0.0, 12, 0.0,
                20, 4, 4.0, 4, 1, 4, 4, 0.0, 16, 0.0,
                21, 5, None, 4, 1, None, None, None, 0, None,
                22, 6, 6.0, 4, 1, 6, 6, 0.0, 24, 0.0,
                23, 7, 7.0, 4, 1, 7, 7, 0.0, 28, 0.0,
                24, 8, 8.0, 4, 1, 8, 8, 0.0, 32, 0.0,  # 8
                25, 9, None, 4, 1, None, None, None, 0, None,
                26, 10, None, 4, 1, None, None, None, 0, None,
                27, 11, 11.0, 4, 1, 11, 11, 0.0, 44, 0.0,
                28, 12, 12.0, 4, 1, 12, 12, 0.0, 48, 0.0,
                29, 13, None, 4, 1, None, None, None, 0, None,
                30, 14, 14.0, 4, 1, 14, 14, 0.0, 56, 0.0,
                31, 15, None, 4, 1, None, None, None, 0, None]

    # group by None, aggregate on column containing 'None'.
    # avg, count, count_distinct, max, min, stdev, sum, var
    results6 = [6.8, 128, 11, 14, 1, 4.24085016554255, 544, 17.98481012658228]

    # group by single column, aggregate on numeric column
    # group_by, avg, count, count_distinct, max, min, stdev, sum, var
    results1_str = \
        [u'Aquamarine', 0.0, 4, 1, 0, 0, 0.0, 0, 0.0,
         u'Azure', 1.0, 4, 1, 1, 1, 0.0, 4, 0.0,
         u'Beige', 2.0, 4, 1, 2, 2, 0.0, 8, 0.0,
         u'Charcoal', 3.0, 4, 1, 3, 3, 0.0, 12, 0.0,
         u'Chartreuse', 4.0, 4, 1, 4, 4, 0.0, 16, 0.0,
         u'Coral', 5.0, 4, 1, 5, 5, 0.0, 20, 0.0,
         u'Crimson', 6.0, 4, 1, 6, 6, 0.0, 24, 0.0,
         u'Cyan', 7.0, 4, 1, 7, 7, 0.0, 28, 0.0,
         u'Forest Green', 8.0, 4, 1, 8, 8, 0.0, 32, 0.0,
         u'Fuchsia', 9.0, 4, 1, 9, 9, 0.0, 36, 0.0,
         u'Golden', 10.0, 4, 1, 10, 10, 0.0, 40, 0.0,
         u'Goldenrod', 11.0, 4, 1, 11, 11, 0.0, 44, 0.0,
         u'Gray', 12.0, 4, 1, 12, 12, 0.0, 48, 0.0,
         u'Hot Pink', 13.0, 4, 1, 13, 13, 0.0, 52, 0.0,
         u'Indigo', 14.0, 4, 1, 14, 14, 0.0, 56, 0.0,
         u'Ivory ', 15.0, 4, 1, 15, 15, 0.0, 60, 0.0,
         u'Khaki', 16.0, 4, 1, 16, 16, 0.0, 64, 0.0,
         u'Lavender', 17.0, 4, 1, 17, 17, 0.0, 68, 0.0,
         u'Lime', 18.0, 4, 1, 18, 18, 0.0, 72, 0.0,
         u'Maroon', 19.0, 4, 1, 19, 19, 0.0, 76, 0.0,
         u'Mauve', 20.0, 4, 1, 20, 20, 0.0, 80, 0.0,
         u'Medium Blue', 21.0, 4, 1, 21, 21, 0.0, 84, 0.0,
         u'Navy Blue', 22.0, 4, 1, 22, 22, 0.0, 88, 0.0,
         u'Olive', 23.0, 4, 1, 23, 23, 0.0, 92, 0.0,
         u'Plum', 24.0, 4, 1, 24, 24, 0.0, 96, 0.0,
         u'Puce', 25.0, 4, 1, 25, 25, 0.0, 100, 0.0,
         u'Royal Blue', 26.0, 4, 1, 26, 26, 0.0, 104, 0.0,
         u'Salmon', 27.0, 4, 1, 27, 27, 0.0, 108, 0.0,
         u'Silver', 28.0, 4, 1, 28, 28, 0.0, 112, 0.0,
         u'Tan', 29.0, 4, 1, 29, 29, 0.0, 116, 0.0,
         u'Teal', 30.0, 4, 1, 30, 30, 0.0, 120, 0.0,
         u'Wheat', 31.0, 4, 1, 31, 31, 0.0, 124, 0.0]

    # group by single column, aggregate on column for data type tested
    # group_by, count, count_distinct, max, min
    results2_str = \
        [u'Aquamarine', 4, 1, u'Aquamarine', u'Aquamarine',
         u'Azure', 4, 1, u'Azure', u'Azure',
         u'Beige', 4, 1, u'Beige', u'Beige',
         u'Charcoal', 4, 1, u'Charcoal', u'Charcoal',
         u'Chartreuse', 4, 1, u'Chartreuse', u'Chartreuse',
         u'Coral', 4, 1, u'Coral', u'Coral',
         u'Crimson', 4, 1, u'Crimson', u'Crimson',
         u'Cyan', 4, 1, u'Cyan', u'Cyan',
         u'Forest Green', 4, 1, u'Forest Green', u'Forest Green',
         u'Fuchsia', 4, 1, u'Fuchsia', u'Fuchsia',
         u'Golden', 4, 1, u'Golden', u'Golden',
         u'Goldenrod', 4, 1, u'Goldenrod', u'Goldenrod',
         u'Gray', 4, 1, u'Gray', u'Gray',
         u'Hot Pink', 4, 1, u'Hot Pink', u'Hot Pink',
         u'Indigo', 4, 1, u'Indigo', u'Indigo',
         u'Ivory ', 4, 1, u'Ivory ', u'Ivory ',
         u'Khaki', 4, 1, u'Khaki', u'Khaki',
         u'Lavender', 4, 1, u'Lavender', u'Lavender',
         u'Lime', 4, 1, u'Lime', u'Lime',
         u'Maroon', 4, 1, u'Maroon', u'Maroon',
         u'Mauve', 4, 1, u'Mauve', u'Mauve',
         u'Medium Blue', 4, 1, u'Medium Blue', u'Medium Blue',
         u'Navy Blue', 4, 1, u'Navy Blue', u'Navy Blue',
         u'Olive', 4, 1, u'Olive', u'Olive',
         u'Plum', 4, 1, u'Plum', u'Plum',
         u'Puce', 4, 1, u'Puce', u'Puce',
         u'Royal Blue', 4, 1, u'Royal Blue', u'Royal Blue',
         u'Salmon', 4, 1, u'Salmon', u'Salmon',
         u'Silver', 4, 1, u'Silver', u'Silver',
         u'Tan', 4, 1, u'Tan', u'Tan',
         u'Teal', 4, 1, u'Teal', u'Teal',
         u'Wheat', 4, 1, u'Wheat', u'Wheat']

    # group by two columns, aggregate on column for data type tested
    # group_by1, group_by2, count, count_distinct, max, min
    results3_str = \
        [u'Aquamarine', 0, 4, 1, u'Aquamarine', u'Aquamarine',
         u'Azure', 1, 4, 1, u'Azure', u'Azure',
         u'Beige', 2, 4, 1, u'Beige', u'Beige',
         u'Charcoal', 3, 4, 1, u'Charcoal', u'Charcoal',
         u'Chartreuse', 4, 4, 1, u'Chartreuse', u'Chartreuse',
         u'Coral', 5, 4, 1, u'Coral', u'Coral',
         u'Crimson', 6, 4, 1, u'Crimson', u'Crimson',
         u'Cyan', 7, 4, 1, u'Cyan', u'Cyan',
         u'Forest Green', 8, 4, 1, u'Forest Green', u'Forest Green',
         u'Fuchsia', 9, 4, 1, u'Fuchsia', u'Fuchsia',
         u'Golden', 10, 4, 1, u'Golden', u'Golden',
         u'Goldenrod', 11, 4, 1, u'Goldenrod', u'Goldenrod',
         u'Gray', 12, 4, 1, u'Gray', u'Gray',
         u'Hot Pink', 13, 4, 1, u'Hot Pink', u'Hot Pink',
         u'Indigo', 14, 4, 1, u'Indigo', u'Indigo',
         u'Ivory ', 15, 4, 1, u'Ivory ', u'Ivory ',
         u'Khaki', 0, 4, 1, u'Khaki', u'Khaki',
         u'Lavender', 1, 4, 1, u'Lavender', u'Lavender',
         u'Lime', 2, 4, 1, u'Lime', u'Lime',
         u'Maroon', 3, 4, 1, u'Maroon', u'Maroon',
         u'Mauve', 4, 4, 1, u'Mauve', u'Mauve',
         u'Medium Blue', 5, 4, 1, u'Medium Blue', u'Medium Blue',
         u'Navy Blue', 6, 4, 1, u'Navy Blue', u'Navy Blue',
         u'Olive', 7, 4, 1, u'Olive', u'Olive',
         u'Plum', 8, 4, 1, u'Plum', u'Plum',
         u'Puce', 9, 4, 1, u'Puce', u'Puce',
         u'Royal Blue', 10, 4, 1, u'Royal Blue', u'Royal Blue',
         u'Salmon', 11, 4, 1, u'Salmon', u'Salmon',
         u'Silver', 12, 4, 1, u'Silver', u'Silver',
         u'Tan', 13, 4, 1, u'Tan', u'Tan',
         u'Teal', 14, 4, 1, u'Teal', u'Teal',
         u'Wheat', 15, 4, 1, u'Wheat', u'Wheat']

    # group by None, aggregate on column for data type tested
    # count, count_distinct, max, min
    results4_str = [128, 32, u'Wheat', u'Aquamarine']

    # group by 'colors', aggregate on column containing 'None'.
    # group_by, count, count_distinct, max, min
    results5_str = \
        [u'Aquamarine', 4, 1, None, None,
         u'Azure', 4, 1, u'1', u'1',
         u'Beige', 4, 1, u'2', u'2',
         u'Charcoal', 4, 1, u'3', u'3',
         u'Chartreuse', 4, 1, u'4', u'4',
         u'Coral', 4, 1, None, None,
         u'Crimson', 4, 1, u'6', u'6',
         u'Cyan', 4, 1, u'7', u'7',
         u'Forest Green', 4, 1, u'8', u'8',
         u'Fuchsia', 4, 1, None, None,
         u'Golden', 4, 1, None, None,
         u'Goldenrod', 4, 1, u'11', u'11',
         u'Gray', 4, 1, u'12', u'12',
         u'Hot Pink', 4, 1, None, None,
         u'Indigo', 4, 1, u'14', u'14',
         u'Ivory ', 4, 1, None, None,
         u'Khaki', 4, 1, None, None,
         u'Lavender', 4, 1, u'1', u'1',
         u'Lime', 4, 1, u'2', u'2',
         u'Maroon', 4, 1, u'3', u'3',
         u'Mauve', 4, 1, u'4', u'4',
         u'Medium Blue', 4, 1, None, None,
         u'Navy Blue', 4, 1, u'6', u'6',
         u'Olive', 4, 1, u'7', u'7',
         u'Plum', 4, 1, u'8', u'8',
         u'Puce', 4, 1, None, None,
         u'Royal Blue', 4, 1, None, None,
         u'Salmon', 4, 1, u'11', u'11',
         u'Silver', 4, 1, u'12', u'12',
         u'Tan', 4, 1, None, None,
         u'Teal', 4, 1, u'14', u'14',
         u'Wheat', 4, 1, None, None]

    # group by two columns, aggregate on column containing 'None'.
    # group_by1, group_by_2, count, count_distinct, max, min
    results6_str = \
        [u'Aquamarine', 0, 4, 1, None, None,
         u'Azure', 1, 4, 1, u'1', u'1',
         u'Beige', 2, 4, 1, u'2', u'2',
         u'Charcoal', 3, 4, 1, u'3', u'3',
         u'Chartreuse', 4, 4, 1, u'4', u'4',
         u'Coral', 5, 4, 1, None, None,
         u'Crimson', 6, 4, 1, u'6', u'6',
         u'Cyan', 7, 4, 1, u'7', u'7',
         u'Forest Green', 8, 4, 1, u'8', u'8',
         u'Fuchsia', 9, 4, 1, None, None,
         u'Golden', 10, 4, 1, None, None,
         u'Goldenrod', 11, 4, 1, u'11', u'11',
         u'Gray', 12, 4, 1, u'12', u'12',
         u'Hot Pink', 13, 4, 1, None, None,
         u'Indigo', 14, 4, 1, u'14', u'14',
         u'Ivory ', 15, 4, 1, None, None,
         u'Khaki', 0, 4, 1, None, None,
         u'Lavender', 1, 4, 1, u'1', u'1',
         u'Lime', 2, 4, 1, u'2', u'2',
         u'Maroon', 3, 4, 1, u'3', u'3',
         u'Mauve', 4, 4, 1, u'4', u'4',
         u'Medium Blue', 5, 4, 1, None, None,
         u'Navy Blue', 6, 4, 1, u'6', u'6',
         u'Olive', 7, 4, 1, u'7', u'7',
         u'Plum', 8, 4, 1, u'8', u'8',
         u'Puce', 9, 4, 1, None, None,
         u'Royal Blue', 10, 4, 1, None, None,
         u'Salmon', 11, 4, 1,  u'11',  u'11',
         u'Silver', 12, 4, 1, u'12', u'12',
         u'Tan', 13, 4, 1, None, None,
         u'Teal', 14, 4, 1, u'14', u'14',
         u'Wheat', 15, 4, 1, None, None]

    # These results seem a bit strange at first but remember - it's unicode!
    # group by None, aggregate on column containing 'None'.
    # count, count_distinct, max, min
    results7_str = [128, 11, u'8', u'1']

    # Exercise histogram facility,
    #   especially cutoff boundaries.
    results_hist1_str = \
        [None, 3/12.0, 5/12.0, 4/12.0,
         1, 0.5, 0.5, 0.0,
         2, 0.5, 0.5, 0.0,
         3, 0.5, 0.5, 0.0,
         4, 0.5, 0.5, 0.0,
         6, 0.5, 0.5, 0.0,
         7, 0.5, 0.5, 0.0,
         8, 0.5, 0.5, 0.0,
         11, 0.0, 0.5, 0.5,
         12, 0.0, 0.5, 0.5,
         14, 0.0, 0.5, 0.5]

    results_hist2_str = \
        [None, 2/11.0, 5/11.0, 4/11.0,
         1, 0.0, 1.0, 0.0,
         2, 0.0, 1.0, 0.0,
         3, 0.0, 1.0, 0.0,
         4, 0.0, 1.0, 0.0,
         6, 0.5, 0.5, 0.0,
         7, 0.5, 0.5, 0.0,
         8, 0.5, 0.5, 0.0,
         11, 0.0, 0.5, 0.5,
         12, 0.0, 0.5, 0.5,
         14, 0.0, 0.5, 0.5]

    results_hist3_str = \
        [None, 4/12.0, 5/12.0, 3/12.0,
         1, 0.5, 0.5, 0.0,
         2, 0.5, 0.5, 0.0,
         3, 0.5, 0.5, 0.0,
         4, 0.5, 0.5, 0.0,
         6, 0.5, 0.5, 0.0,
         7, 0.5, 0.5, 0.0,
         8, 0.5, 0.5, 0.0,
         11, 0.0, 0.5, 0.5,
         12, 0.0, 0.5, 0.5,
         14, 0.0, 0.5, 0.5]

    results_hist4_str = \
        [None, 3/11.0, 5/11.0, 3/11.0,
         1, 0.0, 1.0, 0.0,
         2, 0.0, 1.0, 0.0,
         3, 0.0, 1.0, 0.0,
         4, 0.0, 1.0, 0.0,
         6, 0.5, 0.5, 0.0,
         7, 0.5, 0.5, 0.0,
         8, 0.5, 0.5, 0.0,
         11, 0.0, 0.5, 0.5,
         12, 0.0, 0.5, 0.5,
         14, 0.0, 0.5, 0.5]

    def setUp(self):
        """Initialize test data at beginning of test"""

        # Call parent set-up.
        super(GroupByTest, self).setUp()

        # Create data files
        self.schema_colors = [("Int32_0_15", ta.int32),
                              ("Int32_0_31", ta.int32),
                              ("colors", unicode),
                              ("Int64_0_15", ta.int64),
                              ("Int64_0_31", ta.int64),
                              ("Float32_0_15", ta.float32),
                              ("Float32_0_31", ta.float32),
                              ("Float64_0_15", ta.float64),
                              ("Float64_0_31", ta.float64)]

        self.dataset = "colors_32_9cols_128rows.csv"

        # Data frame creation
        self.frame_colors = frame_utils.build_frame(
            self.dataset, self.schema_colors, self.prefix)

        # Quick sanity check on frame import (row count & schema)
        self.assertEqual(self.frame_colors.row_count,
                         self.expected_rows,
                         "ERROR -- frame_colors row count:  expected: {0},"
                         " actual: {1}."
                         .format(self.expected_rows,
                                 self.frame_colors.row_count))

        self.assertEqual(self.frame_colors.schema,
                         self.schema_colors,
                         "ERROR -- frame_colors schema:  expected: {0},"
                         " actual: {1}."
                         .format(self.schema_colors,
                                 self.frame_colors.schema))

        self.group_by_udf_data = [
            [1, "alpha", 3.0, "small", 1, 3.0, 9],
            [1, "bravo", 5.0, "medium", 1, 4.0, 9],
            [1, "alpha", 5.0, "large", 1, 8.0, 8],
            [2, "bravo", 8.0, "large", 1, 5.0, 7],
            [2, "charlie", 12.0, "medium", 1, 6.0, 6],
            [2, "bravo", 7.0, "small", 1, 8.0, 5],
            [2, "bravo", 12.0, "large",  1, 6.0, 4]
        ]

    def group_by_tst(self, frame, columns, agg_args, results):
        """Test and compare frames containing numeric values."""
        stats = frame.group_by(columns, agg_args)
        stats.name = common_utils.get_a_name(self.prefix)
        stats_flat = common_utils.flatten(sorted(stats.take(stats.row_count)))

        for x, y, i in zip(results, stats_flat, count()):
            self.assertAlmostEqual(x,
                                   y,
                                   places=13,  # stat variation in 14th digit
                                   msg="results[{2}]: Expected: {0} "
                                       "Actual: {1}".format(x, y, i))

    def group_by_tst_str(self, frame, columns, agg_args, results):
        """Test and compare frames containing strings."""
        stats = frame.group_by(columns, agg_args)
        stats.name = common_utils.get_a_name(self.prefix)
        stats_flat = common_utils.flatten(sorted(stats.take(stats.row_count)))

        self.assertEqual(results,
                         stats_flat,
                         "\nExpected:\n{0}\nActual:\n{1}".format(results,
                                                                 stats_flat))

    def test_group_by_int32(self):
        """
        Tests frame.group_by method for int32 data type
        """
        # Exercise normal operation of the group_by argument and group_by
        # aggregation_arguments for a frame containing integers (int32);
        # modified within the test to also include +/- Inf and NaN
        #
        # group_by argument
        # - single column
        # - multiple columns
        # - None
        #
        # aggregation_arguments
        # - avg
        # - count
        # - count_distinct
        # - max
        # - min
        # - sum
        # - var

        def make_it_interesting(row):
            if row["Int32_0_15"] % 5 == 0:      # ending in 0 or 5
                return np.inf
            if row["Int32_0_15"] % 10 == 9:     # ending in 9
                return -np.inf
            if row["Int32_0_15"] % 14 == 13:    # only 13 for this dataset
                return np.nan
            return row["Int32_0_15"]

        # Copy the frame and add +/- Infinity & NaN.
        test_frame = self.frame_colors.copy()
        test_frame.name = common_utils.get_a_name(self.prefix)  # for clean-up
        test_frame.add_columns(make_it_interesting, ('IncNones', ta.int32))

        # Group by a single column
        self.group_by_tst(test_frame,
                          'Int32_0_15',
                          {'Int32_0_31': [ta.agg.avg,
                                          ta.agg.count,
                                          ta.agg.count_distinct,
                                          ta.agg.max,
                                          ta.agg.min,
                                          ta.agg.stdev,
                                          ta.agg.sum,
                                          ta.agg.var]},
                          self.results1)

        # Group_by multiple columns
        self.group_by_tst(test_frame,
                          ['Int32_0_31', 'Int32_0_15'],
                          {'Int32_0_31': [ta.agg.avg,
                                          ta.agg.count,
                                          ta.agg.count_distinct,
                                          ta.agg.max,
                                          ta.agg.min,
                                          ta.agg.stdev,
                                          ta.agg.sum,
                                          ta.agg.var]},
                          self.results2)

        # Group by None
        self.group_by_tst(test_frame,
                          None,
                          {'Int32_0_31': [ta.agg.avg,
                                          ta.agg.count,
                                          ta.agg.count_distinct,
                                          ta.agg.max,
                                          ta.agg.min,
                                          ta.agg.stdev,
                                          ta.agg.sum,
                                          ta.agg.var]},
                          self.results3)

        # Group by single column, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          'Int32_0_15',
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results4)

        # Group by multiple columns, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          ['Int32_0_31', 'Int32_0_15'],
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results5)

        # Group by None, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          None,
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results6)

        #  Confirm test_frame still the same, ignoring added column.
        frame_utils.compare_frames(self.frame_colors, test_frame,
                                   zip(self.frame_colors.column_names,
                                   [u'Int32_0_15',
                                    u'Int32_0_31',
                                    u'colors',
                                    u'Int64_0_15',
                                    u'Int64_0_31',
                                    u'Float32_0_15',
                                    u'Float32_0_31',
                                    u'Float64_0_15',
                                    u'Float64_0_31']))

        # Quick test to catch and verify import failures planted in test data.
        # TODO: Does this belong here or should it be moved to an import test
        # suite?
        if self.frame_colors.get_error_frame():
            fails = self.frame_colors.get_error_frame()
            fail_results = \
                [u'2nd and 3rd columns are equal on 131072 rows.',
                 u'java.lang.IllegalArgumentException: Expected 3 columns, '
                 u'but got 1 columns in this row.',
                 u'Third column is greater than 2nd column 917504 times.',
                 u'java.lang.IllegalArgumentException: Expected 3 columns, '
                 u'but got 1 columns in this row.',
                 u'We have 1048576 entries.',
                 u'java.lang.IllegalArgumentException: Expected 3 columns, '
                 u'but got 1 columns in this row.']

            fails_flat = [num for item in sorted(fails.take(fails.row_count))
                          for num in item]
            self.assertEqual(fails_flat,
                             fail_results,
                             "ERROR -- fails flattened:  expected: {0},"
                             " actual: {1}"
                             .format(fail_results, fails_flat))
            ta.drop_frames(fails)

    def test_group_by_int64(self):
        """
        Tests frame.group_by method for int64 data type
        """

        # Exercise normal operation of the group_by argument and group_by
        # aggregation_arguments for a frame containing integers (int64);
        # modified within the test to also include +/- Inf and NaN
        #
        # group_by argument
        # - single column
        # - multiple columns
        # - None
        #
        # aggregation_arguments
        # - avg
        # - count
        # - count_distinct
        # - max
        # - min
        # - sum
        # - var

        def make_it_interesting(row):
            if row["Int32_0_15"] % 5 == 0:      # ending in 0 or 5
                return np.inf
            if row["Int32_0_15"] % 10 == 9:     # ending in 9
                return -np.inf
            if row["Int32_0_15"] % 14 == 13:    # only 13 for this dataset
                return np.nan
            return row["Int32_0_15"]

        # Copy the frame and add +/- Infinity & NaN.
        test_frame = self.frame_colors.copy()
        test_frame.name = common_utils.get_a_name(self.prefix)  # for clean-up
        test_frame.add_columns(make_it_interesting, ('IncNones', ta.int64))

        # Group_by a single column
        self.group_by_tst(test_frame,
                          'Int64_0_15',
                          {'Int64_0_31': [ta.agg.avg,
                                          ta.agg.count,
                                          ta.agg.count_distinct,
                                          ta.agg.max,
                                          ta.agg.min,
                                          ta.agg.stdev,
                                          ta.agg.sum,
                                          ta.agg.var]},
                          self.results1)

        # Group_by multiple columns
        self.group_by_tst(test_frame,
                          ['Int64_0_31', 'Int64_0_15'],
                          {'Int64_0_31': [ta.agg.avg,
                                          ta.agg.count,
                                          ta.agg.count_distinct,
                                          ta.agg.max,
                                          ta.agg.min,
                                          ta.agg.stdev,
                                          ta.agg.sum,
                                          ta.agg.var]},
                          self.results2)

        # Group by None
        self.group_by_tst(test_frame,
                          None,
                          {'Int64_0_31': [ta.agg.avg,
                                          ta.agg.count,
                                          ta.agg.count_distinct,
                                          ta.agg.max,
                                          ta.agg.min,
                                          ta.agg.stdev,
                                          ta.agg.sum,
                                          ta.agg.var]},
                          self.results3)

        # Group by single column, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          'Int64_0_15',
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results4)

        # Group by multiple columns, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          ['Int64_0_31', 'Int64_0_15'],
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results5)

        # Group by None, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          None,
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results6)

        #  Confirm test_frame still the same. Ignore added column.
        frame_utils.compare_frames(self.frame_colors, test_frame,
                                   zip(self.frame_colors.column_names,
                                   [u'Int32_0_15',
                                    u'Int32_0_31',
                                    u'colors',
                                    u'Int64_0_15',
                                    u'Int64_0_31',
                                    u'Float32_0_15',
                                    u'Float32_0_31',
                                    u'Float64_0_15',
                                    u'Float64_0_31']))

    def test_group_by_float32(self):
        """
        Tests frame.group_by method for float32 data type
        """

        # Exercise normal operation of the group_by argument and group_by
        # aggregation_arguments for a frame containing floats (float32);
        # modified within the test to also include +/- Inf and NaN
        #
        # group_by argument
        # - single column
        # - multiple columns
        # - None
        #
        # aggregation_arguments
        # - avg
        # - count
        # - count_distinct
        # - max
        # - min
        # - sum
        # - var

        def make_it_interesting(row):
            if row["Int32_0_15"] % 5 == 0:      # ending in 0 or 5
                return np.inf
            if row["Int32_0_15"] % 10 == 9:     # ending in 9
                return -np.inf
            if row["Int32_0_15"] % 14 == 13:    # only 13 for this dataset
                return np.nan
            return row["Int32_0_15"]

        # Copy the frame and add +/- Infinity & NaN.
        test_frame = self.frame_colors.copy()
        test_frame.name = common_utils.get_a_name(self.prefix)  # for clean-up
        test_frame.add_columns(make_it_interesting, ('IncNones', ta.float32))

        # Group_by a single column
        self.group_by_tst(test_frame,
                          'Float32_0_15',
                          {'Float32_0_31': [ta.agg.avg,
                                            ta.agg.count,
                                            ta.agg.count_distinct,
                                            ta.agg.max,
                                            ta.agg.min,
                                            ta.agg.stdev,
                                            ta.agg.sum,
                                            ta.agg.var]},
                          self.results1)

        # Group_by multiple columns
        self.group_by_tst(test_frame,
                          ['Float32_0_31', 'Float32_0_15'],
                          {'Float32_0_31': [ta.agg.avg,
                                            ta.agg.count,
                                            ta.agg.count_distinct,
                                            ta.agg.max,
                                            ta.agg.min,
                                            ta.agg.stdev,
                                            ta.agg.sum,
                                            ta.agg.var]},
                          self.results2)

        # Group by None
        self.group_by_tst(test_frame,
                          None,
                          {'Float32_0_31': [ta.agg.avg,
                                            ta.agg.count,
                                            ta.agg.count_distinct,
                                            ta.agg.max,
                                            ta.agg.min,
                                            ta.agg.stdev,
                                            ta.agg.sum,
                                            ta.agg.var]},
                          self.results3)

        # Group by single column, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          'Float32_0_15',
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results4)

        # Group by multiple columns, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          ['Float32_0_31', 'Float32_0_15'],
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results5)

        # Group by None, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          None,
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results6)

        #  Confirm test_frame still the same, ignoring added column.
        frame_utils.compare_frames(self.frame_colors, test_frame,
                                   zip(self.frame_colors.column_names,
                                   [u'Int32_0_15',
                                    u'Int32_0_31',
                                    u'colors',
                                    u'Int64_0_15',
                                    u'Int64_0_31',
                                    u'Float32_0_15',
                                    u'Float32_0_31',
                                    u'Float64_0_15',
                                    u'Float64_0_31']))

    def test_group_by_float64(self):
        """
        Tests frame.group_by method for float64 data type
        """

        # Exercise normal operation of the group_by argument and group_by
        # aggregation_arguments for a frame containing floats (float64;
        # modified within the test to also include +/- Inf and NaN
        #
        # group_by argument
        # - single column
        # - multiple columns
        # - None
        #
        # aggregation_arguments
        # - avg
        # - count
        # - count_distinct
        # - max
        # - min
        # - sum
        # - var

        def make_it_interesting(row):
            if row["Int32_0_15"] % 5 == 0:      # ending in 0 or 5
                return np.inf
            if row["Int32_0_15"] % 10 == 9:     # ending in 9
                return -np.inf
            if row["Int32_0_15"] % 14 == 13:    # only 13 for this dataset
                return np.nan
            return row["Int32_0_15"]

        # Copy the frame and add +/- Infinity & NaN.
        test_frame = self.frame_colors.copy()
        test_frame.name = common_utils.get_a_name(self.prefix)  # for clean-up
        test_frame.add_columns(make_it_interesting, ('IncNones', ta.float64))

        # Group_by a single column
        self.group_by_tst(test_frame,
                          'Float64_0_15',
                          {'Float64_0_31': [ta.agg.avg,
                                            ta.agg.count,
                                            ta.agg.count_distinct,
                                            ta.agg.max,
                                            ta.agg.min,
                                            ta.agg.stdev,
                                            ta.agg.sum,
                                            ta.agg.var]},
                          self.results1)

        # Group_by multiple columns
        self.group_by_tst(test_frame,
                          ['Float64_0_31', 'Float64_0_15'],
                          {'Float64_0_31': [ta.agg.avg,
                                            ta.agg.count,
                                            ta.agg.count_distinct,
                                            ta.agg.max,
                                            ta.agg.min,
                                            ta.agg.stdev,
                                            ta.agg.sum,
                                            ta.agg.var]},
                          self.results2)

        # Group by None.
        self.group_by_tst(test_frame,
                          None,
                          {'Float64_0_31': [ta.agg.avg,
                                            ta.agg.count,
                                            ta.agg.count_distinct,
                                            ta.agg.max,
                                            ta.agg.min,
                                            ta.agg.stdev,
                                            ta.agg.sum,
                                            ta.agg.var]},
                          self.results3)

        # Group by single column, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          'Float64_0_15',
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results4)

        # Group by multiple columns, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          ['Float64_0_31', 'Float64_0_15'],
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results5)

        # Group by None, aggregate on column containing None.
        self.group_by_tst(test_frame,
                          None,
                          {'IncNones': [ta.agg.avg,
                                        ta.agg.count,
                                        ta.agg.count_distinct,
                                        ta.agg.max,
                                        ta.agg.min,
                                        ta.agg.stdev,
                                        ta.agg.sum,
                                        ta.agg.var]},
                          self.results6)

        #  Confirm test_frame still the same, ignoring added column.
        frame_utils.compare_frames(self.frame_colors, test_frame,
                                   zip(self.frame_colors.column_names,
                                   [u'Int32_0_15',
                                    u'Int32_0_31',
                                    u'colors',
                                    u'Int64_0_15',
                                    u'Int64_0_31',
                                    u'Float32_0_15',
                                    u'Float32_0_31',
                                    u'Float64_0_15',
                                    u'Float64_0_31']))

    def test_group_by_str(self):
        """
        Tests frame.group_by method for str data type
        """
        # Exercise normal operation of the group_by argument and group_by
        # aggregation_arguments for a frame containing strings;
        # modified within the test to also include +/- Inf and NaN
        #
        # group_by argument
        # - single column
        # - multiple columns
        # - None
        #
        # aggregation_arguments
        # - count
        # - count_distinct
        # - max
        # - min

        def make_it_interesting(row):
            if row["Int32_0_15"] % 5 == 0:      # ending in 0 or 5
                return np.inf
            if row["Int32_0_15"] % 10 == 9:     # ending in 9
                return -np.inf
            if row["Int32_0_15"] % 14 == 13:    # only 13 for this dataset
                return np.nan
            return row["Int32_0_15"]

        # Copy the frame and add +/- Infinity & NaN.
        test_frame = self.frame_colors.copy()
        test_frame.name = common_utils.get_a_name(self.prefix)  # for clean-up
        test_frame.add_columns(make_it_interesting, ('IncNones', str))

        # Group_by a single column, aggregate on column containing numbers
        # so statistical functions can be used.
        self.group_by_tst_str(test_frame,
                              'colors',
                              {'Int32_0_31': [ta.agg.avg,
                                              ta.agg.count,
                                              ta.agg.count_distinct,
                                              ta.agg.max,
                                              ta.agg.min,
                                              ta.agg.stdev,
                                              ta.agg.sum,
                                              ta.agg.var]},
                              self.results1_str)

        # Group_by a single column, aggregate on column containing strings.
        self.group_by_tst_str(test_frame,
                              'colors',
                              {'colors': [ta.agg.count,
                                          ta.agg.count_distinct,
                                          ta.agg.max,
                                          ta.agg.min]},
                              self.results2_str)

        # Group_by multiple columns, aggregate on column containing strings.
        self.group_by_tst_str(test_frame,
                              ['colors', 'Int32_0_15'],
                              {'colors': [ta.agg.count,
                                          ta.agg.count_distinct,
                                          ta.agg.max,
                                          ta.agg.min]},
                              self.results3_str)

        # Group by None, aggregate on column containing strings.
        self.group_by_tst_str(test_frame,
                              None,
                              {'colors': [ta.agg.count,
                                          ta.agg.count_distinct,
                                          ta.agg.max,
                                          ta.agg.min]},
                              self.results4_str)

        # Group by single column, aggregate on column containing None.
        self.group_by_tst_str(test_frame,
                              'colors',
                              {'IncNones': [ta.agg.count,
                                            ta.agg.count_distinct,
                                            ta.agg.max,
                                            ta.agg.min]},
                              self.results5_str)

        # Group by multiple columns, aggregate on column containing None.
        self.group_by_tst_str(test_frame,
                              ['colors', 'Int32_0_15'],
                              {'IncNones': [ta.agg.count,
                                            ta.agg.count_distinct,
                                            ta.agg.max,
                                            ta.agg.min]},
                              self.results6_str)

        # Group by None, aggregate on column containing None.
        self.group_by_tst_str(test_frame,
                              None,
                              {'IncNones': [ta.agg.count,
                                            ta.agg.count_distinct,
                                            ta.agg.max,
                                            ta.agg.min]},
                              self.results7_str)

        #  Confirm test_frame still the same, ignoring added column.
        frame_utils.compare_frames(self.frame_colors, test_frame,
                                   zip(self.frame_colors.column_names,
                                   [u'Int32_0_15',
                                    u'Int32_0_31',
                                    u'colors',
                                    u'Int64_0_15',
                                    u'Int64_0_31',
                                    u'Float32_0_15',
                                    u'Float32_0_31',
                                    u'Float64_0_15',
                                    u'Float64_0_31']))

    def test_group_by_errors(self):
        """
        Error tests for frame.group_by.
        """

        # Due to the overhead required, these have not been broken up yet into
        # separate tests.  Once we have parallel execution working, they
        # should be moved into individual tests.

        test_frame = self.frame_colors.copy()
        test_frame.name = common_utils.get_a_name(self.prefix)  # for clean-up

        # Statistical functions on column containing string
        # java.lang.IllegalArgumentException: Invalid aggregation function
        with self.assertRaises(ta.rest.command.CommandServerError):
            test_frame.group_by('colors', {'colors': ta.agg.avg})

        # java.lang.IllegalArgumentException: Invalid aggregation function
        with self.assertRaises(ta.rest.command.CommandServerError):
            test_frame.group_by('colors', {'colors': ta.agg.stdev})

        # java.lang.IllegalArgumentException: Invalid aggregation function
        with self.assertRaises(ta.rest.command.CommandServerError):
            test_frame.group_by('colors', {'colors': ta.agg.sum})

        # java.lang.IllegalArgumentException: Invalid aggregation function
        with self.assertRaises(ta.rest.command.CommandServerError):
            test_frame.group_by('colors', {'colors': ta.agg.var})

        # Invalid column name
        with self.assertRaises(ta.rest.command.CommandServerError):
            test_frame.group_by('InvalidColumnName', {'colors': ta.agg.var})

        # Non-existent aggregation column
        with self.assertRaises(ta.rest.command.CommandServerError):
            test_frame.group_by('colors', {'InvalidColumnName': ta.agg.var})

        # Attribute error
        with self.assertRaises(AttributeError):
            test_frame.group_by('colors', {'colors': ta.agg.invalid_attr})

        #  Confirm test_frame still the same. Ignore added column.
        frame_utils.compare_frames(self.frame_colors, test_frame,
                                   zip(self.frame_colors.column_names,
                                   [u'Int32_0_15',
                                    u'Int32_0_31',
                                    u'colors',
                                    u'Int64_0_15',
                                    u'Int64_0_31',
                                    u'Float32_0_15',
                                    u'Float32_0_31',
                                    u'Float64_0_15',
                                    u'Float64_0_31']))

    def test_lda_example(self):
        """LDA demo from examples directory"""

        frame = frame_utils.build_frame("lda.csv",
                                        schema=[('doc_id', str),
                                                ('word_id', str),
                                                ('word_count', ta.int64)],
                                        skip_header_lines=1)
        print frame.inspect(20)

        model = ta.LdaModel()
        results = model.train(frame,
                              'doc_id', 'word_id', 'word_count',
                              num_topics=2, max_iterations=20)

        doc_results = results['topics_given_doc']
        word_results = results['topics_given_word']
        report = results['report']

        print doc_results.inspect()
        print word_results.inspect()
        print report

        print("compute lda score")
        doc_results.rename_columns({'topic_probabilities': 'lda_results_doc'})
        word_results.rename_columns(
            {'topic_probabilities': 'lda_results_word'})

        frame = frame.join(doc_results,
                           left_on="doc_id", right_on="doc_id",
                           how="left")
        frame = frame.join(word_results,
                           left_on="word_id",
                           right_on="word_id",
                           how="left")

        frame.dot_product(['lda_results_doc'],
                          ['lda_results_word'],
                          'lda_score')
        print frame.inspect(20)

        print("compute histogram of scores")
        word_hist = frame.histogram('word_count', 4)
        lda_hist = frame.histogram('lda_score', 2)
        print "word_hist\n", word_hist
        print "lda_hist\n", lda_hist
        group_frame = frame.group_by(
            'word_id',
            {'word_count': ta.agg.histogram(
                cutoffs=word_hist.cutoffs,
                include_lowest=True,
                strict_binning=False),
             'lda_score': ta.agg.histogram(lda_hist.cutoffs)})
        print group_frame.inspect()

        # Locate the histogram for the word "magic";
        #   verify those values.
        group_take = group_frame.take(group_frame.row_count)
        check_cols = group_frame.column_names
        word_col_index = check_cols.index("word_id")
        hist_col_index = check_cols.index("lda_score_HISTOGRAM")
        word_col = [row[word_col_index] for row in group_take]
        magic_row_index = word_col.index(" magic")
        magic_row = group_take[magic_row_index]
        self.assertEqual(magic_row[hist_col_index], [2.0/3, 1.0/3])

    def test_group_by_histogram_params(self):
        """
        Tests group_by histogram parameter combinations.
        """
        # This also tests basic functionality.

        def make_it_interesting(row):
            if row["Int32_0_15"] % 5 == 0:      # ending in 0 or 5
                return np.inf
            if row["Int32_0_15"] % 10 == 9:     # ending in 9
                return -np.inf
            if row["Int32_0_15"] % 14 == 13:    # only 13 for this dataset
                return np.nan
            return row["Int32_0_15"]

        # Copy the frame and add +/- Infinity & NaN.
        test_frame = self.frame_colors.copy(
            name=common_utils.get_a_name(self.prefix))
        test_frame.drop_columns(['Int64_0_15', 'Int64_0_31',
                                 'Float32_0_15', 'Float32_0_31',
                                 'Float64_0_15', 'Float64_0_31'])
        test_frame.add_columns(make_it_interesting, ('IncNones', ta.int32))
        print test_frame.inspect(test_frame.row_count)

        # Histogram tests
        # Set cutoffs for a variety of parameter combinations
        cutoffs = [5, 10, 25, 1000]
        self.group_by_tst_str(test_frame,
                              'IncNones',
                              {'Int32_0_31': ta.agg.histogram(
                                  cutoffs=cutoffs,
                                  include_lowest=True,
                                  strict_binning=False
                              )},
                              self.results_hist1_str)

        self.group_by_tst_str(test_frame,
                              'IncNones',
                              {'Int32_0_31': ta.agg.histogram(
                                  cutoffs=cutoffs,
                                  include_lowest=True,
                                  strict_binning=True
                              )},
                              self.results_hist2_str)

        self.group_by_tst_str(test_frame,
                              'IncNones',
                              {'Int32_0_31': ta.agg.histogram(
                                  cutoffs=cutoffs,
                                  include_lowest=False,
                                  strict_binning=False
                              )},
                              self.results_hist3_str)

        self.group_by_tst_str(test_frame,
                              'IncNones',
                              {'Int32_0_31': ta.agg.histogram(
                                  cutoffs=cutoffs,
                                  include_lowest=False,
                                  strict_binning=True
                              )},
                              self.results_hist4_str)

    def test_group_by_histogram_error(self):
        """
        Tests group_by histogram error cases
        """
        # ta.errors.show_details = True
        # ta.loggers.set_api()

        # Copy the frame and add +/- Infinity & NaN.
        test_frame = self.frame_colors.copy(
            name=common_utils.get_a_name(self.prefix))
        test_frame.drop_columns(['Int64_0_15', 'Int64_0_31',
                                 'Float32_0_15', 'Float32_0_31',
                                 'Float64_0_15', 'Float64_0_31'])

        cutoffs = [5, 10, 25, 1000]
        # Empty cutoff list
        self.assertRaises(ta.rest.command.CommandServerError,
                          test_frame.group_by,
                          ('Int32_0_15',
                           {'Int32_0_31': ta.agg.histogram(
                               cutoffs=[]
                           )}))

        # Cutoff list has only one value
        self.assertRaises(ta.rest.command.CommandServerError,
                          test_frame.group_by,
                          ('Int32_0_15',
                           {'Int32_0_31': ta.agg.histogram(
                               cutoffs=[5]
                           )}))

        # Cutoffs out of order
        self.assertRaises(ta.rest.command.CommandServerError,
                          test_frame.group_by,
                          ('Int32_0_15',
                           {'Int32_0_31': ta.agg.histogram(
                               cutoffs=[5, 25, 10, 1000]
                           )}))

        # Cutoffs wrong type for column
        self.assertRaises(ta.rest.command.CommandServerError,
                          test_frame.group_by,
                          ('Int32_0_15',
                           {'colors': ta.agg.histogram(
                               cutoffs=cutoffs
                           )}))

        # Cutoffs have wrong structure
        self.assertRaises(ta.rest.command.CommandServerError,
                          test_frame.group_by,
                          ('Int32_0_15',
                           {'Int32_0_31': ta.agg.histogram(
                               cutoffs={5: 5, 10: 10, 25: 25, 1000: 1000}
                           )}))

        # Cutoffs not numeric
        # self.assertRaises(ValueError,
        #                   test_frame.group_by,
        #                   'Int32_0_15',
        #                   {'colors': ta.agg.histogram(
        #                       cutoffs=['abalone', 'ecru', 'puce', 'yellow']
        #                   )})

    def test_udf_agg_types(self):
        # Validate example from documentation

        expected_take = [   # Requires 2 additional columns when DPNG-5345 is fixed.
                            [1, u'bravo', 5, 5.0, 1, 1.0, u'r'],
                            [2, u'charlie', 12, 12.0, 2, 0.5, u'a'],
                            [1, u'alpha', 8, 15.0, 2, 1.0, u'll'],
                            [2, u'bravo', 27, 672.0, 6, 0.125, u'aaa']
        ]

        schema = [("a", int), ("b", str), ("c", ta.float64), ("d", str), ("e", int), ("f", ta.float64), ("g", int)]
        frame = ta.Frame(ta.UploadRows(self.group_by_udf_data, schema))
        print frame.inspect()

        print "group_by plugin with custom aggregator function"

        def custom_agg(acc, row):
            acc.c_sum += row.c
            acc.c_prod *= row.c
            acc.a_sum += row.a
            acc.a_div /= row.a
            acc.b_name += row.b[row.a]

        sum_prod_frame = frame.group_by(
            ['a', 'b'],
            ta.agg.udf(aggregator=custom_agg,
                       output_schema=[('c_sum', ta.int64),
                                      ('c_prod', ta.float64),
                                      ('a_sum', ta.int32),
                                      ('a_div', ta.float32),
                                      ('b_name', str)
                       ],
                       init_values=[0, 1, 0, 1, ""]))
                       # init_values=[0, 1, ""]))
        sum_prod_frame.sort('c_prod')
        print sum_prod_frame.inspect()
        actual_take = sum_prod_frame.take(sum_prod_frame.row_count)
        actual_take.sort(key=itemgetter(3))

        self.assertEquals(actual_take, expected_take)

    def test_udf_agg_corner_case(self):
        # Validate example from documentation

        schema = [("a", int), ("b", str), ("c", ta.float64), ("d", str), ("e", int), ("f", ta.float64), ("g", int)]
        frame = ta.Frame(ta.UploadRows(self.group_by_udf_data, schema))
        print frame.inspect()

        print "group_by plugin with custom aggregator function"

        def agg_1_col(acc, row):
            acc.c_sum = acc.c_sum + row.c

        def agg_independent(acc, row):
            # col 'c' running sum;
            #   product is independent of further initialization.
            acc.c_sum = acc.c_sum + row.c
            acc.c_prod += acc.c_sum * row.c

        # Add only 1 column
        frame_1_col = frame.group_by(
            ['a', 'b'],
            ta.agg.udf(aggregator=agg_1_col,
                       output_schema=[('c_sum', ta.float64)],
                       init_values=[0]))

        take_1_col = frame_1_col.take(frame_1_col.row_count)
        print take_1_col
        take_1_col.sort(key=itemgetter(2))
        print take_1_col

        # Supply initialization values for only dependent variables.
        # DPNG-5348
        # sum_prod_frame = frame.group_by(
        #     ['a', 'b'],
        #     ta.agg.udf(aggregator=agg_independent,
        #                output_schema=[('c_sum', ta.float64),
        #                               ('c_prod', ta.float64)],
        #                init_values=[0]))
        #
        # print sum_prod_frame
        # actual_take = sum_prod_frame.take(sum_prod_frame.row_count)
        # actual_take.sort(key=itemgetter(3))

    def test_udf_agg_error(self):
        # Validate example from documentation

        schema = [("a", int), ("b", str), ("c", ta.float64), ("d", str), ("e", int), ("f", ta.float64), ("g", int)]
        frame = ta.Frame(ta.UploadRows(self.group_by_udf_data, schema))
        print frame.inspect()

        print "group_by plugin with custom aggregator function"

        def custom_agg(acc, row):
            # col 'c' running sum and product
            acc.c_sum = acc.c_sum + row.c
            acc.c_prod = acc.c_prod * row.c

        def agg_0_col():
            pass

        # Supply initialization value of wrong type: expect error
        self.assertRaises(ValueError,
                          frame.group_by,
                          ['a', 'b'],
                          ta.agg.udf(aggregator=custom_agg,
                                     output_schema=[('c_sum', ta.float64),
                                                    ('c_prod', ta.float64)],
                                     init_values=[0, ""]))

        # Supply too few initialization values: expect error
        # DPNG-5346
        self.assertRaises(ta.rest.command.CommandServerError,
                          frame.group_by,
                          ['a', 'b'],
                          ta.agg.udf(aggregator=custom_agg,
                                     output_schema=[('c_sum', ta.float64),
                                                    ('c_prod', ta.float64)],
                                     init_values=[0]))

        # Supply too many initialization values: expect warning
        # DPNG-5347
        sum_prod_frame = frame.group_by(
            ['a', 'b'],
            ta.agg.udf(aggregator=custom_agg,
                       output_schema=[('c_sum', ta.float64),
                                      ('c_prod', ta.float64)],
                       init_values=[0, 1, ""]))

        # Add no column: expect error
        self.assertRaises(ValueError,
                          frame.group_by,
                          ['a', 'b'],
                          ta.agg.udf(aggregator=agg_0_col,
                                     output_schema=[],
                                     init_values=[]))


if __name__ == "__main__":
    unittest.main()
