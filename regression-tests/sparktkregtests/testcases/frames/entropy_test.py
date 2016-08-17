""" Test Shannon entropy calculations """
import unittest
import math
from sparktkregtests.lib import sparktk_test


class EntropyTest(sparktk_test.SparkTKTestCase):

    def test_entropy_coin_flip(self):
        """ Get entropy on balanced coin flip. """
        # initialize data and expected result
        frame_load = 10 * [['H'], ['T']]
        expected = math.log(2)

        # create the frame
        frame = self.context.frame.create(frame_load,
                schema=[("data", str)])

        # call the entropy function
        computed_entropy = frame.entropy("data")

        # test that we get the expected result
        self.assertAlmostEqual(computed_entropy,
                expected, delta=.001)

    def test_entropy_exponential(self):
        """ Get entropy on exponential distribution. """
        frame_load = [[0, 1], [1, 2], [2, 4], [4, 8]]
        # Expected result from an on-line entropy calculator in base 2
        expected = 1.640223928941852 * math.log(2)

        # create frame
        frame = self.context.frame.create(frame_load,
                schema=[("data", int), ("weight", int)])

        # call the entropy function to calculate
        computed_entropy = frame.entropy("data", "weight")

        # compare our sparktk result with the expected result
        self.assertAlmostEqual(computed_entropy, expected)


if __name__ == '__main__':
    unittest.main()
