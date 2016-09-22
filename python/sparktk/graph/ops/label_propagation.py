
def label_propagation(self, max_iterations):
    """

    Parameters
    ----------

    Assigns label based off of proximity to different vertices. The labels
    are initially 1 unique label per vertex (the vertex id), and as the
    algorithm runs some of these get erased

    Note this algorithm is neither guaranteed to converge, nor guaranteed to
    converge to the correct value.

    This calls graph frames label propagation which can be found at 

    http://graphframes.github.io/api/scala/index.html#org.graphframes.lib.LabelPropagation

    :return: (Frame) Frame containing the vertex id's and the community they are a member of

    Examples
    --------

        >>> vertex_schema = [('id', int)]
        >>> vertex_rows = [ [1], [2], [3], [4], [5] ]

        >>> edge_rows = [ [1, 2], [1, 3], [2, 3], [1, 4], [4, 5] ]
        >>> edge_schema = [('src', int), ('dst', int)]

        >>> vertex_frame = tc.frame.create(vertex_rows, vertex_schema)
        >>> edge_frame = tc.frame.create(edge_rows, edge_schema)

        >>> graph = tc.graph.create(vertex_frame, edge_frame)

        >>> result = graph.label_propagation(10)
        >>> result.inspect()
        [#]  id  label
        ==============
        [0]   1      1
        [1]   2      2
        [2]   3      2
        [3]   4      2
        [4]   5      1

    """
    from sparktk.frame.frame import Frame
    return Frame(self._tc, self._scala.labelPropagation(max_iterations))
