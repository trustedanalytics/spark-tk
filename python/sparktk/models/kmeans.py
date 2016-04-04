from sparktk.propobj import PropertiesObject

class KMeans(object):

    @staticmethod
    def train(tc, frame, columns, k=2, scalings=None, max_iter=20, epsilon=1e-4, seed=None, init_mode="k-means||"):
        _scala_obj = tc.sc._jvm.org.trustedanalytics.at.models.kmeans.KMeans
        scala_columns = tc.jutils.convert.to_scala_vector_string(columns)
        if scalings:
            scala_scalings = tc.jutils.convert.to_scala_vector_double(scalings)
            scala_scalings = tc.jutils.convert.to_scala_option(scala_scalings)
        else:
            scala_scalings = tc.jutils.convert.to_scala_option(None)

        seed = seed if seed is None else long(seed)
        scala_seed = tc.jutils.convert.to_scala_option(seed)
        scala_model = _scala_obj.train(frame._scala, scala_columns, k, scala_scalings, max_iter, epsilon, init_mode, scala_seed)
        return KMeansModel(tc, columns, scalings, scala_model)


class KMeansModel(PropertiesObject):

    def __init__(self, tc, columns, scalings, scala_model):
        self._tc = tc
        self._columns = columns
        self._scalings = scalings
        self._scala = scala_model

    @property
    def columns(self):
        #return list(self._scala.columns())  todo - get the from-scala to convert back to python
        return self._columns

    @property
    def scalings(self):
        # return list(self._scala.scalings())  todo - get the from-scala to convert back to python
        return self._scalings

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
        return [list(item) for item in list(self._scala.centroids())]

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
