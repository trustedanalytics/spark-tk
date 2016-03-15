from sparktk.simple import SimpleObj

class KMeansModel(SimpleObj):

    def __init__(self, context, frame, columns, scalings, k=2, maxIterations=20, epsilon=1e-4, initializationMode = "k-means||"):
        self._context = context
        self._columns = columns
        self._scalings = scalings
        _scala_obj = context.sc._jvm.org.trustedanalytics.at.model.KMeans
        c = context.jconvert.to_scala_vector_string(columns)
        s = context.jconvert.to_scala_vector_double(scalings)
        self._scala = _scala_obj.train(frame._scala, c, s, k, maxIterations, epsilon, initializationMode)

    @property
    def columns(self):
        #return list(self._scala.columns())
        return self._columns

    @property
    def scalings(self):
        # return list(self._scala.scalings())
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
    def sizes(self):
        return list(self._scala.sizes())

    @property
    def centroids(self):
        return [list(item) for item in list(self._scala.centroids())]

    @property
    def ssew(self):
        return self._scala.ssew()

    def predict(self, frame, columns=None):
        if columns is not None:
            columns = self._context.jconvert.to_scala_vector_string(columns)
        c = self._context.jconvert.to_option(columns)
        self._scala.predict(frame._scala, c)
