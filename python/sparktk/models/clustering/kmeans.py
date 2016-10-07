from sparktk.loggers import log_load; log_load(__name__); del log_load

from sparktk.propobj import PropertiesObject
from sparktk import TkContext

__all__ = ["train", "load", "KMeansModel"]

def train(frame, columns, k=2, scalings=None, max_iter=20, epsilon=1e-4, seed=None, init_mode="k-means||"):
    """
    Creates a KMeansModel by training on the given frame

    :param frame: (Frame) frame of training data
    :param columns: (List[str]) names of columns containing the observations for training
    :param k: (Optional (int)) number of clusters
    :param scalings: (Optional(List[float])) column scalings for each of the observation columns.  The scaling value is multiplied by
     the corresponding value in the observation column
    :param max_iter: (Optional(int)) number of iterations for which the algorithm should run
    :param epsilon: (Optional(float)) distance threshold within which we consider k-means to have converged. Default is 1e-4.
     If all centers move less than this Euclidean distance, we stop iterating one run
    :param seed: Optional(long) seed for randomness
    :param init_mode: (Optional(str)) the initialization technique for the algorithm.   It can be either "random" to choose
     random points as initial clusters or "k-means||" to use a parallel variant of k-means++. Default is "k-means||
    :return: (KMeansModel) trained KMeans model

    """
    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    if isinstance(columns, basestring):
        columns = [columns]
    scala_columns = tc.jutils.convert.to_scala_vector_string(columns)
    if scalings:
        scala_scalings = tc.jutils.convert.to_scala_vector_double(scalings)
        scala_scalings = tc.jutils.convert.to_scala_option(scala_scalings)
    else:
        scala_scalings = tc.jutils.convert.to_scala_option(None)

    seed = seed if seed is None else long(seed)
    scala_seed = tc.jutils.convert.to_scala_option(seed)
    scala_model = _scala_obj.train(frame._scala, scala_columns, k, scala_scalings, max_iter, epsilon, init_mode, scala_seed)
    return KMeansModel(tc, scala_model)


def load(path, tc=TkContext.implicit):
    """load KMeansModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, KMeansModel)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.clustering.kmeans.KMeansModel


class KMeansModel(PropertiesObject):
    """
    A trained KMeans model

    Example
    -------

        >>> frame = tc.frame.create([[2, "ab"],
        ...                          [1,"cd"],
        ...                          [7,"ef"],
        ...                          [1,"gh"],
        ...                          [9,"ij"],
        ...                          [2,"kl"],
        ...                          [0,"mn"],
        ...                          [6,"op"],
        ...                          [5,"qr"]],
        ...                         [("data", float), ("name", str)])

        >>> model = tc.models.clustering.kmeans.train(frame, ["data"], 3, seed=5)

        >>> model.k
        3

        >>> sizes = model.compute_sizes(frame)

        >>> sizes
        [2, 2, 5]

        >>> wsse = model.compute_wsse(frame)

        >>> wsse
        5.3

        >>> model.predict(frame)

        >>> frame.inspect()
        [#]  data  name  cluster
        ========================
        [0]   2.0  ab          1
        [1]   1.0  cd          1
        [2]   7.0  ef          0
        [3]   1.0  gh          1
        [4]   9.0  ij          0
        [5]   2.0  kl          1
        [6]   0.0  mn          1
        [7]   6.0  op          2
        [8]   5.0  qr          2


        >>> model.add_distance_columns(frame)

        >>> frame.inspect()
        [#]  data  name  cluster  distance0  distance1  distance2
        =========================================================
        [0]   2.0  ab          1       36.0       0.64      12.25
        [1]   1.0  cd          1       49.0       0.04      20.25
        [2]   7.0  ef          0        1.0      33.64       2.25
        [3]   1.0  gh          1       49.0       0.04      20.25
        [4]   9.0  ij          0        1.0      60.84      12.25
        [5]   2.0  kl          1       36.0       0.64      12.25
        [6]   0.0  mn          1       64.0       1.44      30.25
        [7]   6.0  op          2        4.0      23.04       0.25
        [8]   5.0  qr          2        9.0      14.44       0.25

        >>> model.columns
        [u'data']

        >>> model.scalings  # None


        >>> centroids = model.centroids

        >>> model.save("sandbox/kmeans1")

        >>> restored = tc.load("sandbox/kmeans1")

        >>> restored.centroids == centroids
        True

        >>> restored_sizes = restored.compute_sizes(frame)

        >>> restored_sizes == sizes
        True

    <hide>
    >>> restored2 = tc.models.clustering.kmeans.load("sandbox/kmeans1")

    >>> restored.centroids == centroids
    True

    >>> restored.predict(frame)
    [#]  data  name  cluster  cluster_0
    ===================================
    [0]   2.0  ab          1          1
    [1]   1.0  cd          1          1
    [2]   7.0  ef          0          0
    [3]   1.0  gh          1          1
    [4]   9.0  ij          0          0
    [5]   2.0  kl          1          1
    [6]   0.0  mn          1          1
    [7]   6.0  op          2          2
    [8]   5.0  qr          2          2


    </hide>
    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        return KMeansModel(tc, scala_model)

    @property
    def columns(self):
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.columns()))

    @property
    def scalings(self):
        s = self._tc.jutils.convert.from_scala_option(self._scala.scalings())
        if s:
            return list(self._tc.jutils.convert.from_scala_seq(s))
        return None

    @property
    def k(self):
        return self._scala.k()

    @property
    def max_iterations(self):
        return self._scala.maxIterations()

    @property
    def initialization_mode(self):
        return self._scala.initializationMode()

    @property
    def centroids(self):
        return [list(item) for item in list(self._scala.centroidsAsArrays())]

    def compute_sizes(self, frame, columns=None):
        c = self.__columns_to_option(columns)
        return [int(n) for n in self._scala.computeClusterSizes(frame._scala, c)]

    def compute_wsse(self, frame, columns=None):
        c = self.__columns_to_option(columns)
        return self._scala.computeWsse(frame._scala, c)

    def predict(self, frame, columns=None):
        c = self.__columns_to_option(columns)
        self._scala.predict(frame._scala, c)

    def add_distance_columns(self, frame, columns=None):
        c = self.__columns_to_option(columns)
        self._scala.addDistanceColumns(frame._scala, c)

    def __columns_to_option(self, columns):
        if isinstance(columns, basestring):
            columns = [columns]
        if columns is not None:
            columns = self._tc.jutils.convert.to_scala_vector_string(columns)
        return self._tc.jutils.convert.to_scala_option(columns)

    def save(self, path):
        if isinstance(path, basestring):
            self._scala.save(self._tc._scala_sc, path)

    def export_to_mar(self, path):
        """ export the trained model to MAR format for Scoring Engine """
        if isinstance(path, basestring):
            return self._scala.exportToMar(self._tc._scala_sc, path)

del PropertiesObject
