from sparktk.loggers import log_load; log_load(__name__); del log_load
from sparktk.propobj import PropertiesObject
import os

def train():
    """
    Creates a GaussianMixtureModel by training on the given frame

    :param frame: (Frame) frame of training data
    :param observation_columns: (List(str)) names of columns containing the observations for training
    :param column_scalings: (Optional(List(float))) column scalings for each of the observation columns.  The scaling
        value is multiplied by the corresponding value in the observation column
    :param k: (int) number of clusters
    :param max_iterations: (int) number of iterations for which the algorithm should run
    :param convergence_tol:  (float) Largest change in log-likelihood at which convergence is considered to have occurred
    :param seed: (int) seed for randomness
    :return: GaussianMixtureModel

    """

    return None


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.clustering.gmm.GaussianMixtureModel


class GaussianMixtureModel(PropertiesObject):
    """
    A trained GaussianMixtureModel model

    Example
    -------

        >>> import numpy as np
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

        >>> frame.inspect()
        [#]  data  name
        ===============
        [0]   2  ab
        [1]   1  cd
        [2]   7  ef
        [3]   1  gh
        [4]   9  ij
        [5]   2  kl
        [6]   0  mn
        [7]   6  op
        [8]   5  qr

        >>> model = tc.models.clustering.gmm.train(frame, ["data"], [1.0], 4)

        >>> model.k
        4

        >>> cluster_sizes = model.cluster_sizes.values()

        >>> cluster_sizes.sort()

        >>> cluster_sizes
        [4, 5]

        >>> d = [[s.encode('ascii') for s in list] for list in model.gaussians]

        >>> mu =[]
        >>> sigma = []
        >>> for i in d:
        ...     mu.append((i[0].partition('[')[2]).partition(']')[0])
        ...     sigma.append((i[1].partition('List(List(')[2]).partition('))')[0])
        >>> l = []
        >>> for i in range(0, len(mu)):
        ...     l.append([float(mu[i]), float(sigma[i])])
        >>> len(l)
        4
        >>> l
        []
        >>> l.sort(key=lambda x: x[0])
        >>> np.allclose(np.array(l),np.array([[1.1984454608177824, 0.5599200477022921],
        ... [6.6173304476544335, 2.1848346923369246],
        ... [6.79969916638852, 2.2623755196701305]]),atol=1e-00)
        True

        >>> model.predict(frame)

        >>> frame.inspect()
        [#]  data  name  predicted_cluster
        ==================================
        [0]   9.0  ij                    0
        [1]   2.0  ab                    1
        [2]   0.0  mn                    1
        [3]   5.0  qr                    0
        [4]   7.0  ef                    0
        [5]   1.0  cd                    1
        [6]   1.0  gh                    1
        [7]   6.0  op                    0
        [8]   2.0  kl                    1


        >>> model.observation_columns
        [u'data']

        >>> model.column_scalings
        [1.0]

        >>> model.save("sandbox/GaussianMixtureModel")

        >>> restored = tc.load("sandbox/GaussianMixtureModel")

        >>> model.cluster_sizes == restored.cluster_sizes
        True

    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def load(tc, scala_model):
        return GaussianMixtureModel(tc, scala_model)

    @property
    def observation_columns(self):
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.observationColumns()))

    @property
    def column_scalings(self):
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.columnScalings()))

    @property
    def k(self):
        return self._scala.k()

    @property
    def max_iterations(self):
        return self._scala.maxIterations()

    @property
    def convergence_tol(self):
        return self._scala.convergenceTol()

    @property
    def seed(self):
        return self._scala.seed()

    @property
    def gaussians(self):
        g = self._tc.jutils.convert.from_scala_seq(self._scala.gaussians())
        results = []
        for i in g:
            results.append(self._tc.jutils.convert.from_scala_seq(i))
        return results

    @property
    def cluster_sizes(self):
        return self._tc.jutils.convert.scala_map_to_python(self._scala.computedGmmClusterSize())

    def predict(self,
                frame,
                source_column,
                destination_column,
                similarity_column,
                k,
                max_iterations,
                initialization_mode):
        tc = frame._tc
        _scala_obj = get_scala_obj(tc)
        scala_model = self._scala.predict(frame._scala,
                                       source_column,
                                       destination_column,
                                       similarity_column,
                                       k,
                                       max_iterations,
                                       initialization_mode)

    def __columns_to_option(self, c):
        if c is not None:
            c = self._tc.jutils.convert.to_scala_vector_string(c)
        return self._tc.jutils.convert.to_scala_option(c)


    def save(self, path):
        self._scala.save(self._tc._scala_sc, path)

del PropertiesObject
