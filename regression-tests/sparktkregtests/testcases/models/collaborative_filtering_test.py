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

""" Tests collaborative filtering against a generated data set"""
import unittest

from sparktkregtests.lib import sparktk_test


class CollabFilterTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(CollabFilterTest, self).setUp()

        ALS_dataset = self.get_file("collab_filtering.csv")
        schema = [("user", str), ("product", str), ("rating", float)]

        self.frame = self.context.frame.import_csv(ALS_dataset, schema=schema)

        # add integer columns for user and product
        # this is caused by a weird api requirement
        self.frame.add_columns(
            lambda x: [x["user"][5:], x['product'][5:]],
            [("user_int", int), ("item_int", int)])
        self.base_frame = self.frame.to_pandas(self.frame.count())

        self.old_frame = self.frame.copy()
        # Remove some baseline values, collaborative filtering has
        # to have empty entries
        self.frame.filter(lambda x: (x["item_int"]+x["user_int"]) % 4 > 0)

    def test_collaborative_filtering_recommend(self):
        """Test collaborative filtering and recommend"""
        model = self.context.models.recommendation \
            .collaborative_filtering \
            .train(self.frame, "user_int", "item_int", "rating", max_steps=15)
        recommend = model.recommend(0, 40)
        recommend_dict = {i['product']: i['rating'] for i in recommend}

        for k, v in recommend_dict.iteritems():
            self.assertAlmostEqual(
                self.base_frame[
                    (self.base_frame["product"] == "item-"+str(k)) &
                    (self.base_frame['user'] == "user-0")]['rating'].values[0],
                v, delta=3.0)

    def test_collaborative_filtering_model_parameters(self):
        """Test collaborative filtering model parameters"""
        model = self.context.models.recommendation \
            .collaborative_filtering \
            .train(self.frame,
                   "user_int",
                   "item_int",
                   "rating",
                   15,
                   0.7,
                   0.8,
                   3,
                   False,
                   7,
                   5,
                   8,
                   0.4)

        self.assertEqual(model.source_column_name, "user_int")
        self.assertEqual(model.dest_column_name, "item_int")
        self.assertEqual(model.weight_column_name, "rating")
        self.assertEqual(model.max_steps, 15)
        self.assertEqual(model.regularization, 0.7)
        self.assertEqual(model.alpha, 0.8)
        self.assertEqual(model.num_factors, 3)
        self.assertEqual(model.use_implicit, False)
        self.assertEqual(model.num_user_blocks, 7)
        self.assertEqual(model.num_item_block, 5)
        self.assertEqual(model.checkpoint_iterations, 8)
        self.assertEqual(model.target_rmse, 0.4)

    @unittest.skip("many steps fails, likely a checkpointing issue")
    def test_als_collaborative_filtering_many_steps(self):
        """ Test collaborative filtering with many steps"""
        model = self.context.models.recommendation \
            .collaborative_filtering \
            .train(self.frame, "user_int", "item_int", "rating", max_steps=125)
        recommend = model.recommend(0, 40)
        recommend_dict = {i['product']: i['rating'] for i in recommend}

        for k, v in recommend_dict.iteritems():
            self.assertAlmostEqual(
                self.base_frame[
                    (self.base_frame["product"] == "item-"+str(k)) &
                    (self.base_frame['user'] == "user-0")]['rating'].values[0],
                v, delta=3.0)

    def test_collaborative_filtering_predict(self):
        """Test collaborative filtering and predict"""
        model = self.context.models.recommendation \
            .collaborative_filtering \
            .train(self.frame, "user_int", "item_int", "rating", max_steps=15)
        scores = model.predict(
            self.old_frame, "user_int", "item_int")

        pd_scores = scores.to_pandas(scores.count())
        for _, i in pd_scores.iterrows():
            item_val = "item-"+str(int(i['product']))
            user_val = "user-"+str(int(i['user']))
            self.assertAlmostEqual(
                self.base_frame[
                    (self.base_frame["product"] == item_val) &
                    (self.base_frame['user'] == user_val)]['rating'].values[0],
                i['rating'], delta=5.5)

    def test_collaborative_filtering_invalid_user(self):
        """Test collaborative filtering train with invalid user"""
        with self.assertRaisesRegexp(
                Exception,
                'requirement failed: column invalid_user was not found'):
            self.context.models.recommendation \
                .collaborative_filtering \
                .train(self.frame, "invalid_user", "item_int", "rating")

    def test_collaborative_filtering_invalid_item(self):
        """Test collaborative filtering train with invalid item"""
        with self.assertRaisesRegexp(
                Exception,
                'requirement failed: column invalid_int was not found'):
            self.context.models.recommendation \
                .collaborative_filtering \
                .train(self.frame, "user_int", "invalid_int", "rating")

    def test_collaborative_filtering_invalid_rating(self):
        """Test collaborative filtering with invalid rating"""
        with self.assertRaisesRegexp(
                Exception,
                'requirement failed: column invalid_rating was not found'):
            self.context.models.recommendation \
                .collaborative_filtering \
                .train(self.frame, "user_int", "item_int", "invalid_rating")

    def test_collaborative_filtering_invalid_rmse(self):
        """Test collaborative filtering with invalid target_rmse"""
        with self.assertRaisesRegexp(
                Exception,
                'requirement failed: target RMSE must be a positive value'):
            self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int",
                "rating", target_rmse=-15.0)

    def test_collaborative_filtering_invalid_num_item_blocks(self):
        """Test collaborative filtering with invalid num_item_blocks"""
        with self.assertRaisesRegexp( Exception, 'Found num_item_blocks = -15.  Expected non-negative integer.'):
            self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int",
                "rating", num_item_blocks=-15)

    def test_collaborative_filtering_invalid_num_user_blocks(self):
        """Test collaborative filtering with invalid num_user_blocks"""
        with self.assertRaisesRegexp(Exception, 'Found num_user_blocks = -15.  Expected non-negative integer.'):
            self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int",
                "rating", num_user_blocks=-15)

    def test_collaborative_filtering_invalid_checkpoint_iterations(self):
        """Test collaborative filtering with invalid checkpoint_iterations"""
        with self.assertRaisesRegexp(Exception, 'Found checkpoint_iterations = -15.  Expected non-negative integer.'):
            self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int",
                "rating", checkpoint_iterations=-15)

    def test_collaborative_filtering_invalid_max_steps(self):
        """Test collaborative filtering invalid max steps"""
        with self.assertRaisesRegexp(
                Exception,
                'Found max_steps = -15.  Expected non-negative integer.'):
            self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int",
                "rating", max_steps=-15)

    def test_collaborative_filtering_invalid_regularization(self):
        """Test collaborative filtering with invalid regularization"""
        with self.assertRaisesRegexp(
                Exception,
                '\'regularization\' parameter must have a value between 0 and 1'):
            self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int",
                "rating", regularization=-1.0)

        with self.assertRaisesRegexp(
                Exception,
                '\'regularization\' parameter must have a value between 0 and 1'):
            self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int",
                "rating", regularization=41.0)

    def test_collaborative_filtering_invalid_alpha(self):
        """Test collaborative filtering with invalid alpha"""
        with self.assertRaisesRegexp(
                Exception,
                '\'alpha\' parameter must have a value between 0 and 1'):
            self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int", "rating", alpha=-1.0)

        with self.assertRaisesRegexp(
                Exception, '\'alpha\' parameter must have a value between 0 and 1'):
            self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int", "rating", alpha=41.0)

    def test_collaborative_filtering_invalid_recommend_items(self):
        """Test collaborative filtering recommend invalid items"""
        with self.assertRaisesRegexp(
                Exception,
                'Found number_of_recommendations = -10.  Expected non-negative integer.'):
            model = self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int", "rating")
            model.recommend(0, -10)

    def test_collaborative_filtering_invalid_recommend_value(self):
        """Test collaborative filtering invalid item"""
        with self.assertRaisesRegexp(
                Exception,
                'requirement failed: No users found with id = 1000.'):
            model = self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int", "rating")
            model.recommend(1000, 10)

    def test_collaborative_filtering_predict_frame_invalid_source(self):
        """Test collaborative filtering predict frame with invalid source"""
        with self.assertRaisesRegexp(
                Exception,
                'requirement failed: column invalid_source was not found'):
            model = self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int", "rating")
            model.predict(
                self.old_frame, "invalid_source", "item_int")

    def test_collaborative_filtering_predict_frame_invalid_item(self):
        """Test collaborative filtering predict frame with invalid item"""
        with self.assertRaisesRegexp(
                Exception,
                'requirement failed: column invalid_item was not found'):
            model = self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int", "rating")
            model.predict(
                self.old_frame, "user_int", "invalid_item")

    def test_collaborative_filtering_predict_frame_invalid_output_user(self):
        """Test collaborative filtering predict with invalid output user"""
        with self.assertRaisesRegexp(
                Exception, 'requirement failed: column name can\'t be empty'):
            model = self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int", "rating")
            model.predict(
                self.old_frame, "user_int",
                "item_int", output_user_column_name="")

    def test_collaborative_filtering_predict_invalid_output_product(self):
        """Test collaborative filtering predict with invalid output product"""
        with self.assertRaisesRegexp(
                Exception, 'requirement failed: column name can\'t be empty'):
            model = self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int", "rating")
            model.predict(
                self.old_frame, "user_int",
                "item_int", output_product_column_name="")

    def test_collaborative_filtering_predict_frame_invalid_output_rating(self):
        """Test collaborative filtering predict with invalid output rating"""
        with self.assertRaisesRegexp(
                Exception, 'requirement failed: column name can\'t be empty'):
            model = self.context.models.recommendation \
                .collaborative_filtering.train(
                self.frame, "user_int", "item_int", "rating")
            model.predict(
                self.old_frame, "user_int",
                "item_int", output_rating_column_name="")


if __name__ == "__main__":
    unittest.main()
