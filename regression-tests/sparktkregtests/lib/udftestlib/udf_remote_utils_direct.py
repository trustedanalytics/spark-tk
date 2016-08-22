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
   Support file for Remote UDF testing.
   Called directly from test cases;
     calls functions in udf_remote_utils_indirect.py
"""

__author__ = "WDW"
__credits__ = ["Prune Wickart", "Anjali Sood"]
__version__ = "2015.01.12"

from sparktkregtests.lib.udftestlib import udf_remote_utils_indirect


def length(my_str):
    return udf_remote_utils_indirect.length(my_str)


def row_build(row):
    return length(row.letter)


def add_select_col(frame):

    import udf_remote_utils_indirect

    # When the interface is done, this will append utils_indirect.
    # ia.udf.install(['udf_remote_utils_indirect.py'])
    frame.add_columns(
        udf_remote_utils_indirect.selector, ('other_column', int))
