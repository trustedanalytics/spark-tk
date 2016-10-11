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


def loopy_belief_propagation(self, prior, edge_weight, max_iterations=10):
    """

    Performs loopy belief propagation on a graph representing a Potts model. This optimizes based off of
    user provided priors.

    Parameters
    ----------

    :param prior: (String) The name of the column of space delimited string of floats representing the prior distribution on a vertex
    :param edge_weight: (String) The name of the column of weight value on edges
    :param max_iterations: The number of iterations to run for

    Examples
    --------

        >>> vertex_schema = [('id', int), ('label', float), ("prior_val", str), ("was_labeled", int)]
        >>> vertex_rows = [ [1, 1, "0.7 0.3", 1], [2, 1, "0.7 0.3", 1], [3, 5, "0.7 0.3", 0], [4, 5, "0.7 0.3", 0], [5, 5, "0.7 0.3", 1] ]

        >>> edge_schema = [('src', int), ('dst', int), ('weight', int)]
        >>> edge_rows = [ [1, 2, 2], [1, 3, 1], [2, 3, 1], [1, 4, 1], [4, 5, 1] ]

        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)
        >>> vertex_frame.inspect()
        [#]  id  label  prior_val  was_labeled
        ======================================
        [0]   1    1.0  0.7 0.3              1
        [1]   2    1.0  0.7 0.3              1
        [2]   3    5.0  0.7 0.3              0
        [3]   4    5.0  0.7 0.3              0
        [4]   5    5.0  0.7 0.3              1

        >>> result = graph.loopy_belief_propagation("prior_val", "weight", 2)
        >>> result.inspect() 
        [#]  id  label  prior_val  was_labeled
        ======================================
        [0]   1    1.0  0.7 0.3              1
        [1]   2    1.0  0.7 0.3              1
        [2]   3    5.0  0.7 0.3              0
        [3]   4    5.0  0.7 0.3              0
        [4]   5    5.0  0.7 0.3              1
        <BLANKLINE>
        [#]  posterior
        ==============================================
        [0]  [0.9883347610773112,0.011665238922688819]
        [1]  [0.9743014865548763,0.025698513445123698]
        [2]  [0.9396772870897875,0.06032271291021254]
        [3]  [0.9319529856190276,0.06804701438097235]
        [4]  [0.8506957305238876,0.1493042694761125]



    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.loopyBeliefPropagation(prior, edge_weight, max_iterations))
