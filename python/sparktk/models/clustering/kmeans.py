from sparktk.loggers import log_load; log_load(__name__); del log_load

from sparktk.propobj import PropertiesObject


def train(frame, columns, k=2, scalings=None, max_iter=20, epsilon=1e-4, seed=None, init_mode="k-means||"):
    """
    Creates a KMeansModel by training on the given frame

    :param tc: TkContext
    :param frame: frame of training data
    :param columns: names of columns containing the observations for training
    :param k: number of clusters
    :param scalings: column scalings for each of the observation columns.  The scaling value is multiplied by
     the corresponding value in the observation column
    :param max_iter: number of iterations for which the algorithm should run
    :param epsilon: distance threshold within which we consider k-means to have converged. Default is 1e-4.
     If all centers move less than this Euclidean distance, we stop iterating one run
    :param seed: seed for randomness
    :param init_mode: the initialization technique for the algorithm.   It can be either "random" to choose
     random points as initial clusters or "k-means||" to use a parallel variant of k-means++. Default is "k-means||
    :return: KMeansModel

    """
    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
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


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.clustering.kmeans.KMeansModel


class KMeansModel(PropertiesObject):

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def load(tc, scala_model):
        return KMeansModel(tc, scala_model)

    @property
    def columns(self):
        return ["data"] #list(self._scala.columns())  todo - get the from-scala to convert back to python
        #return self._columns

    @property
    def scalings(self):
        return None #self._tc.jutils.convert. list(self._scala.scalings())  todo - get the from-scala to convert back to python
        #return self._scalings

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

    def __columns_to_option(self, c):
        if c is not None:
            c = self._tc.jutils.convert.to_scala_vector_string(c)
        return self._tc.jutils.convert.to_scala_option(c)

    def save(self, path):
        self._scala.save(self._tc._scala_sc, path)

del PropertiesObject
