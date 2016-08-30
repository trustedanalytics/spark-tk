
def page_rank(self, convergence_tolerance=None, reset_probability=None, max_iterations=None):
    """
    **Page Rank**

    Page Rank is a popular statistic that ranks vertices based off of
    connectivity in the global graph

    Exactly 1 of convergence_tolerance and max_iterations must be set (termination criteria)

    Parameters
    ----------

    :convergence_tolerance: (Float) If the difference between successive iterations is less than this, the algorithm terminates. Mutually exclusive with max_iterations
    :reset_probability: (Float) Value for the reset probabiity in the page rank algorithm
    :max_iterations: (Int) Maximum number of iterations the page rank should run before terminating. Mutually exclusive with convergence_tolerance

    :return: (Frame) Frame containing the vertex id's and their page rank 

    Examples
    --------

        >>> vertex_schema = [('id', int)]
        >>> edge_schema = [('src', int), ('dst', int)]

        >>> vertex_rows = [ [1], [2], [3], [4], [5] ]
        >>> edge_rows = [ [1, 2], [1, 3], [2, 3], [1, 4], [4, 5] ]
        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)

        >>> result = graph.page_rank(max_iterations=20)
        >>> result.inspect()
        [#]  PageRank  Vertex
        =======================
        [0]         1      0.15
        [1]         2    0.1925
        [2]         3  0.356125
        [3]         4    0.1925
        [4]         5  0.313625

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.pageRank(
        self._tc.jutils.convert.to_scala_option(max_iterations),
        self._tc.jutils.convert.to_scala_option(reset_probability),
        self._tc.jutils.convert.to_scala_option(convergence_tolerance)))
