from collections import namedtuple

PicResult = namedtuple("PicResult", ["frame", "k", "cluster_sizes"])


def power_iteration_clustering(self, source_column, destination_column, similarity_column, k=2, max_iterations=100,
                               initialization_mode = "random"):
    """
    Power Iteration Clustering finds a low-dimensional embedding of a dataset using truncated power iteration on a
    normalized pair-wise similarity matrix of the data.

    Parameters
    ----------

    :param source_column: (str) Name of the column containing the source node
    :param destination_column: (str) Name of the column containing the destination node
    :param similarity_column: (str) Name of the column containing the similarity
    :param k: (Optional(int)) Number of clusters to cluster the graph into. Default is 2
    :param max_iterations: (Optional(int)) Maximum number of iterations of the power iteration loop. Default is 100
    :param initialization_mode: (Optional(str)) Initialization mode of power iteration clustering. This can be either
     "random" to use a random vector as vertex properties, or "degree" to use normalized sum similarities. Default is "random".
    :return: (namedtuple) Returns namedtuple containing the results frame(node and cluster), k (number of clusters),
     and cluster_sizes(a map of clusters and respective size)

    Example
    -------

        >>> frame = tc.frame.create([[1,2,1.0],
        ...                         [1,3,0.3],
        ...                         [2,3,0.3],
        ...                         [3,0,0.03],
        ...                         [0,5,0.01],
        ...                         [5,4,0.3],
        ...                         [5,6,1.0],
        ...                         [4,6,0.3]],
        ...                         [('Source', int), ('Destination', int), ('Similarity',float)])

        >>> frame.inspect()
        [#]  Source  Destination  Similarity
        ====================================
        [0]       1            2         1.0
        [1]       1            3         0.3
        [2]       2            3         0.3
        [3]       3            0        0.03
        [4]       0            5        0.01
        [5]       5            4         0.3
        [6]       5            6         1.0
        [7]       4            6         0.3

        >>> x = frame.power_iteration_clustering('Source', 'Destination', 'Similarity', k=3)

        >>> x.frame.inspect()
        [#]  id  cluster
        ================
        [0]   4        1
        [1]   0        2
        [2]   6        3
        [3]   2        3
        [4]   1        3
        [5]   3        1
        [6]   5        3

        >>> x.k
        3
        >>> x.cluster_sizes
        {u'2': 1, u'3': 4, u'1': 2}

    """
    result = self._scala.powerIterationClustering(source_column,
                                                  destination_column,
                                                  similarity_column,
                                                  k,
                                                  max_iterations,
                                                  initialization_mode)
    k_val = result.k()
    cluster_sizes = self._tc.jutils.convert.scala_map_to_python(result.clusterSizes())
    from sparktk.frame.frame import Frame
    py_frame = Frame(self._tc, result.clusterMapFrame())
    return PicResult(frame=py_frame, k=k_val, cluster_sizes=cluster_sizes)
