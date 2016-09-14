
def loopy_belief_propagation(self, prior, weight, max_iterations=10):
    """

    Performs loopy belief propagation on a graph representing a Potts model. This optimizes based off of
    user provided priors.

    Parameters
    ----------

    :param prior: (String) The space delimited string of floats representing the prior distribution on a vertex
    :param weight: (String) The name of the weight value on edges
    :param max_iterations: The number of iterations to run for

    Examples
    --------

        >>> vertex_schema = [('id', int), ('label', float), ("prior_val", str), ("wasLabeled", int)]
        >>> edge_schema = [('src', int), ('dst', int), ('weight', int)]

        >>> vertex_rows = [ [1, 1, "0.7 0.3", 1], [2, 1, "0.7 0.3", 1], [3, 5, "0.7 0.3", 0], [4, 5, "0.7 0.3", 0], [5, 5, "0.7 0.3", 1] ]
        >>> edge_rows = [ [1, 2, 2], [1, 3, 1], [2, 3, 1], [1, 4, 1], [4, 5, 1] ]
        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)
        >>> vertex_frame.inspect()
        [#]  id  label  prior_val  wasLabeled
        =====================================
        [0]   1    1.0  0.7 0.3             1
        [1]   2    1.0  0.7 0.3             1
        [2]   3    5.0  0.7 0.3             0
        [3]   4    5.0  0.7 0.3             0
        [4]   5    5.0  0.7 0.3             1

        >>> result = graph.loopy_belief_propagation("prior_val", "weight", 2)
        >>> result.inspect() 
        [#]  id  label  prior_val  wasLabeled  Posterior
        ================================================================================
        [0]   1    1.0  0.7 0.3             1  [0.9883347610773112,0.011665238922688817]
        [1]   2    1.0  0.7 0.3             1  [0.9743014865548763,0.02569851344512369]
        [2]   3    5.0  0.7 0.3             0  [0.9396772870897875,0.06032271291021256]
        [3]   4    5.0  0.7 0.3             0  [0.9319529856190276,0.06804701438097237]
        [4]   5    5.0  0.7 0.3             1  [0.8506957305238877,0.14930426947611242]

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.loopyBeliefPropagation(max_iterations, weight, prior))
