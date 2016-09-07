''' test cases for Pricipal Components Analysis'''
import unittest
import numpy as np
import sys
import os
from sparktkregtests.lib import sparktk_test


class PrincipalComponent(sparktk_test.SparkTKTestCase):
    # expected singular values
    expected_singular_val = [3373.70412657, 594.11385671,
                             588.713470217, 584.157023124,
                             579.433395835, 576.659495077,
                             572.267630461, 568.224352464,
                             567.328732759, 560.882281619]
    # expected right-singular vectors V
    expected_R_singular_vec = \
        [[0.315533916, -0.3942771, 0.258362247, -0.0738539198,
          -0.460673735, 0.0643077298, -0.0837131184, 0.0257963888,
          0.00376728499, 0.669876972],
         [0.316500921, -0.165508013, -0.131017612, 0.581988787,
          -0.0863507191, 0.160473134, 0.53134635, 0.41199152,
          0.0823770991, -0.156517367],
         [0.316777341, 0.244415549, 0.332413311, -0.377379981,
          0.149653873, 0.0606339992, -0.163748261, 0.699502817,
          -0.171189721, -0.124509149],
         [0.318988109, -0.171520719, -0.250278714, 0.335635209,
          0.580901954, 0.160427725, -0.531610364, -0.0304943121,
          -0.0785743304, 0.201591811],
         [0.3160833, 0.000386702461, -0.108022985, 0.167086405,
          -0.470855879, -0.256296677, -0.318727111, -0.155621638,
          -0.521547782, -0.418681224],
         [0.316721742, 0.288319245, 0.499514144, 0.267566455,
          -0.0338341451, -0.134086469, -0.184724393, -0.246523528,
          0.593753078, -0.169969303],
         [0.315335647, -0.258529064, 0.374780341, -0.169762381,
          0.416093803, -0.118232778, 0.445019707, -0.395962728,
          -0.337229123, -0.0937071881],
         [0.314899154, -0.0294147958, -0.447870311, -0.258339192,
          0.0794841625, -0.71141762, 0.110951688, 0.102784186,
          0.292018251, 0.109836478],
         [0.315542865, -0.236497774, -0.289051199, -0.452795684,
          -0.12175352, 0.5265342, -0.0312645934, -0.180142504,
          0.318334436, -0.359303747],
         [0.315875856, 0.72196434, -0.239088332, -0.0259999274,
          -0.0579153559, 0.244335633, 0.232808362, -0.233600306,
          -0.181191102, 0.3413174]]

    def setUp(self):
        super(PrincipalComponent, self).setUp()
        schema = [("X1", int),
                  ("X2", int),
                  ("X3", int),
                  ("X4", int),
                  ("X5", int),
                  ("X6", int),
                  ("X7", int),
                  ("X8", int),
                  ("X9", int),
                  ("X10", int)]
        train_data = self.get_file("pcadata.csv")
        self.frame = self.context.frame.import_csv(train_data, schema=schema)

    def test_pca_train_mean(self):
        """Test the train functionality with mean centering"""
        pca_model = self.context.models.dimreduction.pca.train(
            self.frame,
            ["X1", "X2", "X3", "X4", "X5",
             "X6", "X7", "X8", "X9", "X10"],
            True, 10)

        # actual right-singular vectors
        actual_R_singular_vec = pca_model.right_singular_vectors

        # actual singular values
        actual_singular_val = pca_model.singular_values

        for c in self.frame.column_names:
            mean = self.frame.column_summary_statistics(c).mean
            self.frame.add_columns(
                lambda x: x[c] - mean, (c+"_n", float))

        pca_model = self.context.models.dimreduction.pca.train(
            self.frame,
            ["X1_n", "X2_n", "X3_n", "X4_n", "X5_n",
             "X6_n", "X7_n", "X8_n", "X9_n", "X10_n"],
            False, 10)

        # actual right-singular vectors
        actual_R_singular_vec_mean = pca_model.right_singular_vectors

        # actual singular values
        actual_singular_val_mean = pca_model.singular_values

        self.assertEqual(
            np.allclose(np.array(actual_singular_val),
                        np.array(actual_singular_val_mean), atol=1e-04), True)
        self.assertEqual(
            np.allclose(np.array(actual_R_singular_vec),
                        np.array(actual_R_singular_vec_mean),
                        atol=1e-04), True)

    def test_pca_predict(self):
        """Test the train functionality"""
        pca_model = self.context.models.dimreduction.pca.train(
            self.frame,
            ["X1", "X2", "X3", "X4", "X5",
             "X6", "X7", "X8", "X9", "X10"],
            False, 10)

        predicted_frame = pca_model.predict(self.frame, False)
        pd_frame = predicted_frame.download(predicted_frame.row_count).data
        actual_R_singular_vec = map(
            list, zip(*pca_model.right_singular_vectors))

        for index, value in pd_frame.iterrows():
            vec1 = i[0:10]
            vec2 = i[10:]
            dot_product = [sum([(r1)*(r2) for r1, r2 in zip(vec1, k)])
                           for k in actual_R_singular_vec]
            for i, j in zip(vec2, dot_product):
                self.assertAlmostEqual(i, j)

    def test_pca_train(self):
        """Test the train functionality"""
        pca_model = self.context.models.dimreduction.pca.train(
            self.frame,
            ["X1", "X2", "X3", "X4", "X5",
             "X6", "X7", "X8", "X9", "X10"],
            False, 10)

        # actual right-singular vectors
        actual_R_singular_vec = pca_model.right_singular_vectors

        # actual singular values
        actual_singular_val = pca_model.singular_values

        self.assertEqual(np.allclose(
            np.array(actual_singular_val),
            np.array(self.expected_singular_val)), True)
        self.assertEqual(np.allclose(np.absolute(
            np.array(actual_R_singular_vec)),
            np.absolute(np.array(self.expected_R_singular_vec)),
            atol=1e-04), True)

    @unittest.skip("")
    def test_pca_publish(self):
        """Test the publish functionality"""
        pca_model = self.context.models.dimreduction.pca.train(
            self.frame,
            ["X1", "X2", "X3", "X4", "X5",
             "X6", "X7", "X8", "X9", "X10"],
            False, 10)
        path = pca_model.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_pca_default(self):
        """Test default no. of k"""
        pca_model = self.context.models.dimreduction.pca.train(
            self.frame,
            ["X1", "X2", "X3", "X4", "X5",
             "X6", "X7", "X8", "X9", "X10"],
            False)

        # actual right-singular vectors
        actual_R_singular_vec = pca_model.right_singular_vectors

        # actual singular values
        actual_singular_val = pca_model.singular_values

        self.assertEqual(np.allclose(
            np.array(actual_singular_val),
            np.array(self.expected_singular_val)), True)
        self.assertEqual(np.allclose(np.absolute(
            np.array(actual_R_singular_vec)),
            np.absolute(np.array(self.expected_R_singular_vec)),
            atol=1e-06), True)

    def test_pca_bad_no_of_k(self):
        """Test invalid k value in train"""
        #with self.assertRaises():
        self.context.models.dimreduction.pca.train(self.frame,
                               ["X1", "X2", "X3", "X4", "X5",
                                "X6", "X7", "X8", "X9", "X10"],
                               11)

    def test_pca_invalid_k(self):
        """Test k < 1 in train"""
        #with self.assertRaises(ta.rest.command.CommandServerError):
        self.context.models.dimreduction.pca.train(self.frame,
                               ["X1", "X2", "X3", "X4", "X5",
                                "X6", "X7", "X8", "X9", "X10"],
                               0)

    def test_pca_bad_column_name(self):
        """Test bad feature column name"""
        #with self.assertRaises(ta.rest.command.CommandServerError):
        self.context.models.dimreduction.pca.train(self.frame,
                               ["ERR", "X2", "X3", "X4", "X5",
                                "X6", "X7", "X8", "X9", "X10"],
                               10)

    def test_pca_bad_column_type(self):
        """Test bad feature column name type"""
        #with self.assertRaises(ta.rest.command.CommandServerError):
        self.context.models.dimreduction.pca.train(self.frame, 10, 10)

    def test_pca_orthogonality(self):
        """Test orthogonality of resulting vectors"""
        pca_model = self.context.models.dimreduction.pca.train(
            self.frame,
            ["X1", "X2", "X3", "X4", "X5",
             "X6", "X7", "X8", "X9", "X10"],
            False, 10)

        # actual right-singular vectors
        actual_R_singular_vec = pca_model.right_singular_vectors

        res_mat = np.array(actual_R_singular_vec)
        res_tran = res_mat.transpose()
        derived_id = np.mat(res_mat)*np.mat(res_tran)
        self.assertEqual(np.allclose(derived_id, np.identity(10)), True)

    def test_pca_singular_values(self):
        """Test for positive singular values"""
        pca_model = self.context.models.dimreduction.pca.train(
            self.frame,
            ["X1", "X2", "X3", "X4", "X5",
             "X6", "X7", "X8", "X9", "X10"],
            False, 10)

        actual_singular_val = pca_train_out['singular_values']

        for val in actual_singular_val:
            self.assertGreaterEqual(val, 0)

if __name__ == '__main__':
    unittest.main()
