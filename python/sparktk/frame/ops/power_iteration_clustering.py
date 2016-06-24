from collections import namedtuple

PicResult = namedtuple("PicResult", ["k", "cluster_sizes"])


def power_iteration_clustering(self, source_column, destination_column, similarity_column, k=2, max_iterations=100, initialization_mode = "random"):
    """

    Parameters
    ----------

    :param source_column: Name of the column containing the source node
    :param destination_column: Name of the column containing the destination node
    :param similarity_column: Name of the column containing the similarity
    :param k: Number of clusters to cluster the graph into. Default is 2
    :param max_iterations: (str) Maximum number of iterations of the power iteration loop. Default is 100
    :param initialization_mode: Initialization mode of power iteration clustering. This can be either "random" to use a
     random vector as vertex properties, or "degree" to use normalized sum similarities. Default is "random".

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

        >>> frame.inspect()
        [#]  id  cluster
        ================
        [0]   4        1
        [1]   0        2
        [2]   6        3
        [3]   2        3
        [4]   1        3
        [5]   3        1
        [6]   5        3

        >>> x[0]
        3
        >>> x[1]
        {u'Cluster:2': 1, u'Cluster:1': 2, u'Cluster:3': 4}

    """
    #return self._tc.jutils.convert.from_scala_seq(
    result = self._scala.powerIterationClustering(source_column,
                                                  destination_column,
                                                  similarity_column,
                                                  k,
                                                  max_iterations,
                                                  initialization_mode)
    k_val = result.k()
    cluster_sizes = self._tc.jutils.convert.scala_map_to_python(result.clusterSizes())
    return PicResult(k=k_val, cluster_sizes=cluster_sizes)
