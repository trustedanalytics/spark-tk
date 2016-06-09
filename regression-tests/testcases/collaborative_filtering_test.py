# #############################################################################
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
Usage: python2.7 collaborative_filtering_test.py

Checks the two algorithms for collaborative filtering work, data set
is built to only have 10 possible recommendations, one of which is liked
by the user.

PRNG seed is not static.
"""
__author__ = ["TEV", "Venkatesh", "lcoatesx"]
__credits__ = ["TEV", "Venkatesh", "Karen Herrold"]
__version__ = "2015.01.27.002"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import trustedanalytics as ia
import pandas

from qalib import frame_utils
from qalib import atk_test


class CollabFilterTest(atk_test.ATKTestCase):

    def setUp(self):
        """Build frames and graphs to be tested."""
        # call parent setup
        super(CollabFilterTest, self).setUp()
        baseline = [
            ['user-0', 'item-0', 18.78],
            ['user-0', 'item-1', 27.64],
            ['user-0', 'item-2', 22.94],
            ['user-0', 'item-3', 22.69],
            ['user-0', 'item-4', 6.21],
            ['user-0', 'item-5', 22.39],
            ['user-0', 'item-6', 27.61],
            ['user-0', 'item-7', 14.10],
            ['user-0', 'item-8', 17.07],
            ['user-0', 'item-9', 19.65],
            ['user-0', 'item-10', 21.75],
            ['user-0', 'item-11', 4.27],
            ['user-0', 'item-12', 23.70],
            ['user-0', 'item-13', 10.93],
            ['user-0', 'item-14', 8.75],
            ['user-0', 'item-15', 19.19],
            ['user-0', 'item-16', 12.43],
            ['user-0', 'item-17', 10.36],
            ['user-0', 'item-18', 28.73],
            ['user-0', 'item-19', 5.041],
            ['user-0', 'item-20', 4.45],
            ['user-0', 'item-21', 14.58],
            ['user-0', 'item-22', 17.36],
            ['user-0', 'item-23', 14.96],
            ['user-0', 'item-24', 19.96],
            ['user-0', 'item-25', 21.94],
            ['user-0', 'item-26', 16.16],
            ['user-0', 'item-27', 19.49],
            ['user-0', 'item-28', 11.68],
            ['user-0', 'item-29', 21.36],
            ['user-0', 'item-30', 1.92],
            ['user-0', 'item-31', 13.75],
            ['user-0', 'item-32', 7.40],
            ['user-0', 'item-33', 24.05],
            ['user-0', 'item-34', 15.41],
            ['user-0', 'item-35', 17.33],
            ['user-0', 'item-36', 13.28],
            ['user-0', 'item-37', 24.38],
            ['user-0', 'item-38', 14.54],
            ['user-0', 'item-39', 15.24],
            ]
        self.baseline = baseline
        self.dataframe = pandas.DataFrame(
            baseline, columns=["user", "product", "rating"])

        ALS_dataset = "collab_filtering.csv"
        schema = [("user", str), ("product", str), ("rating", ia.float32)]

        self.frame = frame_utils.build_frame(ALS_dataset, schema, self.prefix)
        self.frame.add_columns(
            lambda x: x["user"][5:],  ("user_int", ia.int32))
        self.frame.add_columns(
            lambda x: x["product"][5:],  ("item_int", ia.int32))
        self.base_frame = self.frame
        self.frame.filter(lambda x: x["item_int"] % 3 == 0)

    def test_als_collaborative_filtering(self):
        """ Test the als algorithm of collaborative filtering """
        # Run the filter and verify the results
        model = ia.CollaborativeFilteringModel()
        model.train(self.frame, "user_int", "item_int", "rating", max_steps=25)
        recommend = model.recommend(0, 40)

        baseline = {i[1]: i[2] for i in self.baseline}
        for i in recommend:
            self.assertAlmostEqual(
                baseline["item-"+str(i['product'])], i['rating'], delta=3)

    def test_collaborative_filtering_predict(self):
        """ Test the als algorithm of collaborative filtering """
        # Run the filter and verify the results
        model = ia.CollaborativeFilteringModel()
        model.train(self.frame, "user_int", "item_int", "rating", max_steps=25)
        scores = model.predict(self.frame, "user_int", "item_int")

        pd_scores = scores.download(scores.row_count)
        base_frame_pd = self.base_frame.download(self.base_frame.row_count)
        print base_frame_pd
        for _, i in pd_scores.iterrows():
            self.assertAlmostEqual(
                base_frame_pd[(base_frame_pd["product"] == "item-"+str(int(i['product']))) &
                              (base_frame_pd['user'] == "user-"+str(int(i['user'])))]['rating'].tolist()[0], i['rating'], delta=3.5)

    def test_collaborative_filtering_score(self):
        """ Test the als algorithm of collaborative filtering """
        # Run the filter and verify the results
        model = ia.CollaborativeFilteringModel()
        model.train(self.frame, "user_int", "item_int", "rating", max_steps=25)
        base_frame_pd = self.base_frame.download(self.base_frame.row_count).tail(10)
        for _, i in base_frame_pd.iterrows():
            score = model.score(int(i["user"][5:]), int(i["product"][5:]))
            self.assertAlmostEqual(
                i["rating"], score, delta=3)


if __name__ == "__main__":
    unittest.main()
