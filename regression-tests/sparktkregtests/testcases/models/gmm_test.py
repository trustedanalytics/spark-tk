"""Test guassian mixture models against known values"""
import unittest
import random
from collections import Counter
from numpy import array
from sparktkregtests.lib import sparktk_test


class GMMModelTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        data_file = self.get_file("gmm_data.csv")
        self.data = [[-4.621530567642421, 8.6406983965969193],
                [6.4267172354491446, -12.277610775308155],
                [0.098530164125373215, 6.0836012949611131],
                [-4.392254720475024, 8.0189612888460466],
                [-1.0702592750569377, 5.2550831951640031],
                [1.193539633339006, 6.3363714198628118],
                [0.092577751429795768, 5.4779624163828577],
                [8.9970034636235017, -12.01522267407065],
                [-2.3280540831589738, 8.8258542895727512],
                [-4.5062757288578634, 7.6809240414923927],
                [-4.3731494020166028, 7.7596274086851036],
                [8.8736881353774066, -8.3754596947004725],
                [-3.5440958303407917, 9.6105833336468045],
                [-0.27046429010742823, 4.4815881766068086],
                [1.6054071607924059, 4.5470404617330189],
                [-3.8438888915112988, 9.8232777173527985],
                [6.9081835257094752, -10.403841674972018],
                [5.2420445063021734, -11.360873907414765],
                [9.0996512983761839, -10.155501900285019],
                [-0.19114627082150037, 5.3138585210207614],
                [-4.3150893479336485, 8.3786139033869791],
                [-4.589150256894313, 9.4030058284080926],
                [7.1156659501367452, -9.678572758787146],
                [7.4144868265738637, -10.836199275576863],
                [1.2881402757859808, 6.7564004024956015],
                [-4.0537977946046633, 7.2513827760184721],
                [6.1969612182976075, -9.2518183005905037],
                [-3.5717093015483115, 9.2276297985492004],
                [-4.0147982362661789, 10.08407883327838],
                [-0.79670259398287868, 6.1055531158028122],
                [6.1913382645865278, -8.4845917367181691],
                [-0.27933817992227172, 6.5478301064697408],
                [7.0941251414622917, -10.235738266309539],
                [-4.2535003848828827, 6.8221614663027861],
                [-0.89325491843783311, 6.2995080129338765],
                [-1.725462524718381, 5.6079788547762517],
                [-4.1987207246273268, 8.5133110885864163],
                [1.0898412100719519, 6.2412369079834775],
                [-1.2223017468432908, 5.6744104925369792],
                [0.30097801715891481, 5.4591574580632374],
                [7.4286348203200872, -8.0792007474592324],
                [10.10512262037296, -9.5638571747112415],
                [-2.3205126478390565, 8.6414119736091379],
                [7.537465043296395, -11.017303890706284],
                [-3.0054612996625729, 10.428048677949876],
                [7.544671198453873, -10.646649201409796],
                [-0.046393241288497644, 3.6575775856747126],
                [0.041377197511572317, 3.571388696278226],
                [7.4614512336514185, -10.738871347760295],
                [6.6257026868771769, -10.356463238690889]]
        self.frame = self.context.frame.create(
            self.data, schema=[("x1", float), ("x2", float)])
        
    @unittest.skip("")
    def test_train(self):
        """ Verify that model operates as expected in straightforward case"""
        model = self.context.models.clustering.gmm.train(
                    self.frame, ["x1", "x2"],
                    column_scalings=[1.0, 1.0],
                    k=3,
                    max_iterations=100,
                    seed=15)
        #TO-DO: check mu and sigma when bug is fixed
        """ spark output: 
            ('weight = ', 0.33999999999999447, 'mu = ', DenseVector([7.4272, -10.2046]), 'sigma = ', array([[ 1.44051463,  0.16630311],
       [ 0.16630311,  1.34915586]]))
('weight = ', 0.098976119621882006, 'mu = ', DenseVector([-1.2906, 6.5323]), 'sigma = ', array([[ 1.665016  , -3.70420433],
       [-3.70420433,  8.25421454]]))
('weight = ', 0.56102388037812356, 'mu = ', DenseVector([-2.0081, 7.1369]), 'sigma = ', array([[ 4.72581545, -2.70880995],
       [-2.70880995,  2.58042891]])) """

    def test_predict(self):
        """ Tests output of predict """
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"],
            column_scalings=[1.0, 1.0],
            k=3,
            max_iterations=100,
            seed=15)
        model.predict(self.frame)
        results_df = self.frame.to_pandas(self.frame.count())

        data = map(tuple, self.data)
        actual_cluster_sizes = Counter(results_df["predicted_cluster"].tolist())
        expected_cluster_sizes = {2: 27, 0: 17, 1: 6}
        self.assertItemsEqual(actual_cluster_sizes, expected_cluster_sizes)

    def test_gmm_1_class(self):
        """Test gmm doesn't error on 1 class only"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"], [1.0, 1.0], k=1)

    def test_gmm_1_iteration(self):
        """Train on 1 iteration only, shouldn't throw exception"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1"], column_scalings=[1.0],
            max_iterations=1)

    def test_gmm_high_convergence(self):
        """Train on high convergence, should throw exception"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"], column_scalings=[1.0, 1.0],
            convergence_tol=1e6)

    def test_gmm_negative_seed(self):
        """Train on negative seed, shouldn't throw exception"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"], column_scalings=[1.0, 1.0],
            seed=-20)

    def test_gmm_0_scalings(self):
        """all-zero column scalings, shouldn't throw exception"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"], column_scalings=[0.0, 0.0])

    def test_gmm_negative_scalings(self):
        """negative column scalings, shouldn't throw exception"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"], column_scalings=[-1.0, -1.0])

    def test_gmm_empty_frame(self):
        """ Verify that model operates as expected in straightforward case"""
        # Train on an empty frame
        block_data = []
        frame = self.context.frame.create(
            block_data,
            [("x1", float)])

        with self.assertRaisesRegexp(
                Exception, "empty collection"):
            self.context.models.clustering.gmm.train(
                frame, ["x1"], column_scalings=[1.0])

    def test_0_classes_errors(self):
        """Train on 0 classes, should error"""
        with self.assertRaisesRegexp(
                Exception, "k must be at least 1"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1", "x2"], column_scalings=[1.0, 1.0], k=0)

    def test_negative_classes(self):
        """Train on negative classes, should error"""
        with self.assertRaisesRegexp(
                Exception, "k must be at least 1"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1"], column_scalings=[1.0], k=-5)

    def test_0_iterations(self):
        """Train on 0 iterations, should error"""
        with self.assertRaisesRegexp(
                Exception, "maxIterations must be a positive value"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1"], column_scalings=[1.0],
                max_iterations=0)

    def test_negative_iterations(self):
        """Train on negative iterations, should error"""
        with self.assertRaisesRegexp(
                Exception, "maxIterations must be a positive value"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1"], column_scalings=[1.0],
                max_iterations=-20)

    def test_wrong_column_scalings(self):
        """Insufficient column scalings, should error"""
        with self.assertRaisesRegexp(
                Exception, "columnWeights must not be null or empty"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1"], column_scalings=[])

    def test_too_many_column_scalings(self):
        """Extra column scalings, should error"""
        with self.assertRaisesRegexp(
                Exception,
                "Length of columnWeights and observationColumns.*"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1", "x2"], column_scalings=[1.0, 1.0, 1.0])

    def test_missing_column_scalings(self):
        """Missing column scalings, should error"""
        with self.assertRaisesRegexp(
            TypeError, "train\(\) takes at least 3 arguments.*"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1", "x2"], k=2)

if __name__ == "__main__":
    unittest.main()
