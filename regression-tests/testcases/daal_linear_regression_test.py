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
 Tests Linear Regression Model against known values.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class DaalLinearRegression(atk_test.ATKTestCase):

    def setUp(self):
        """Build the frames needed for the tests."""
        super(DaalLinearRegression, self).setUp()
        dataset = "linear_regression_gen.csv"
        schema = [("c1", ia.float64),
                  ("c2", ia.float64),
                  ("c3", ia.float64),
                  ("c4", ia.float64),
                  ("label", ia.float64)]

        self.frame = frame_utils.build_frame(
            dataset, schema, self.prefix)

    def test_model_predict_output(self):
        """Test output format of predict"""
        name = common_utils.get_a_name(self.prefix)
        model = ia.DaalLinearRegressionModel(name)
        res = model.train(self.frame, ['c1', 'c2', 'c3', 'c4'], ["label"])['betas'][0]
        for (i,j) in zip([0, 0.5, -0.7, -0.24, 0.4],res):
            self.assertAlmostEqual(i,j, places=4)
        res = model.predict(self.frame, ['c1', 'c2', 'c3', 'c4'], ["label"])
        pd_res = res.download(res.row_count)
        for _, i in pd_res.iterrows():
            self.assertAlmostEqual(i["label"], i["predict_label"])


if __name__ == '__main__':
    unittest.main()
