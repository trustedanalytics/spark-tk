"""Test interface functionality of frame.sort"""
import unittest

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from qalib import sparktk_test


class FrameSortTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        super(FrameSortTest, self).setUp()

        dataset = self.get_file("movie.csv")
        schema = [("src", int),
                  ("dir", str),
                  ("dest", int),
                  ("weight", int),
                  ("e_type", str)]

        self.frame = self.context.frame.import_csv(dataset, schema=schema, header=True)

    def test_frame_sortedk_col_single_descending(self):
        """ Test single-column sorting descending"""
        topk_frame = self.frame.sorted_k(5, [("weight", False)])
        down_take = topk_frame.take(20)

        for i in range(0, len(down_take) - 1):
            self.assertGreaterEqual(
                down_take.data[i][3], down_take.data[i + 1][3])

    def test_frame_sortedk_col_single_ascending(self):
        """ Test single-column sorting ascending"""
        topk_frame = self.frame.sorted_k(5, [("weight", True)])
        up_take_expl = topk_frame.take(20)

        for i in range(0, len(up_take_expl) - 1):
            self.assertLessEqual(
                up_take_expl.data[i][3], up_take_expl.data[i+1][3])

    def test_frame_sortedk_col_multiple_ascending(self):
        """ Test multiple-column sorting, ascending"""
        topk_frame = self.frame.sorted_k(
            5, [("weight", True), ("e_type", True)])
        up_take = topk_frame.take(20)

        for i in range(0, len(up_take) - 1):
            # If 1st sort key is equal, compare the 2nd
            if up_take[i][3] == up_take[i + 1][3]:
                self.assertLessEqual(up_take.data[i][4], up_take.data[i + 1][4])
            else:
                self.assertLessEqual(
                    up_take.data[i][3], up_take.data[i + 1][3])

    def test_frame_sortedk_col_multiple_descending(self):
        """ Test multiple-column sorting, descending"""
        topk_frame = self.frame.sorted_k(
            5, [("weight", False), ("e_type", False)])
        down_take = topk_frame.take(20)

        for i in range(0, len(down_take) - 1):
            # If 1st sort key is equal, compare the 2nd
            if down_take.data[i][3] == down_take.data[i + 1][3]:
                self.assertGreaterEqual(
                    down_take.data[i][4], down_take.data[i + 1][4])
            else:
                self.assertGreaterEqual(
                    down_take.data[i][3], down_take.data[i + 1][3])

    def test_frame_sortedk_col_multiple_mixed(self):
        """ Test multiple-column sorting, mixed ascending/descending"""
        topk_frame = self.frame.sorted_k(
            5, [("src", False), ("e_type", True), ("weight", True)])
        mixed_take = topk_frame.take(20)

        for i in range(0, len(mixed_take) - 1):
            # If 1st sort key is equal, compare the 2nd
            if mixed_take.data[i][0] == mixed_take.data[i + 1][0]:
                # If 2nd sort key is also equal, compare the 3rd
                if mixed_take.data[i][3] == mixed_take.data[i + 1][3]:
                    self.assertGreaterEqual(
                        mixed_take.data[i][4], mixed_take.data[i + 1][4])
                else:
                    self.assertGreaterEqual(
                        mixed_take.data[i][3], mixed_take.data[i + 1][3])
            else:
                self.assertLessEqual(
                    mixed_take.data[i][0], mixed_take.data[i + 1][0])

    def test_frame_sortedk_bad_k(self):
        """Test sortedk with a bad type of k"""
        with self.assertRaises(Exception):
            self.frame.sorted_k("5", [("weight", False)])

    def test_frame_sortedk_negative_k(self):
        """Test sortedk with a negative k value"""
        with self.assertRaises(Exception):
            self.frame.sorted_k(-1, [("weight", False)])

    def test_frame_sortedk_k_0(self):
        """Test sorted k with k equal to 0"""
        with self.assertRaises(Exception):
            self.frame.sorted_k(0, [("weight", False)])

    def test_frame_sortedk_bad_depth(self):
        """Test sorted k with a tree depth type error"""
        with self.assertRaises(Exception):
            self.frame.sorted_k(5, [("weight", False)], reduce_tree_depth="5")

    def test_frame_sortedk_negative_depth(self):
        """Test sortedk with a negative depth"""
        with self.assertRaises(Exception):
            self.frame.sorted_k(5, [("weight", False)], reduce_tree_depth=-1)

    def test_frame_sorted_k_0_depth(self):
        """test sorted k with a depth of 0"""
        with self.assertRaises(Exception):
            self.frame.sorted_k(5, [("weight", False)], reduce_tree_depth=0)

    def test_frame_sortedk_bad_column(self):
        """Test sorted k errors on bad column"""
        with self.assertRaises(Exception):
            self.frame.sorted_k(5, [('no-such-column', True)])

    def test_frame_sort_typerror(self):
        """Test sort with no arguments raises a type error"""
        with self.assertRaises(TypeError):
            self.frame.sort()


if __name__ == "__main__":
    unittest.main()
