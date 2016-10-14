# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from sparktk.loggers import log_load; log_load(__name__); del log_load
from sparktk.propobj import PropertiesObject
from sparktk import TkContext
import os

__all__ = ["train", "load", "GaussianMixtureModel"]

def train(frame,
          observation_columns,
          column_scalings,
          k=2,
          max_iterations=20,
          convergence_tol=0.01,
          seed=None):
    """
    Creates a GaussianMixtureModel by training on the given frame

    :param frame: (Frame) frame of training data
    :param observation_columns: (List(str)) names of columns containing the observations for training
    :param column_scalings: (List(float)) column scalings for each of the observation columns.  The scaling
        value is multiplied by the corresponding value in the observation column
    :param k: (Optional(int)) number of clusters
    :param max_iterations: (Optional(int)) number of iterations for which the algorithm should run
    :param convergence_tol:  (Optional(float)) Largest change in log-likelihood at which convergence is considered to have occurred
    :param seed: (Optional(int)) seed for randomness
    :return: GaussianMixtureModel

    """   
    if frame is None:
        raise ValueError("frame cannot be None")

    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    seed = int(os.urandom(2).encode('hex'),16) if seed is None else seed
    scala_columns = tc.jutils.convert.to_scala_vector_string(observation_columns)
    scala_scalings = tc.jutils.convert.to_scala_vector_double(column_scalings)
    scala_model = _scala_obj.train(frame._scala,
                                   scala_columns,
                                   scala_scalings,
                                   k,
                                   max_iterations,
                                   convergence_tol,
                                   seed)
    return GaussianMixtureModel(tc, scala_model)


def load(path, tc=TkContext.implicit):
    """load GaussianMixtureModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, GaussianMixtureModel)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.clustering.gmm.GaussianMixtureModel

class Gaussian(PropertiesObject):
    """
    Gaussian sigma and mu values for a trained GaussianMixtureModel
    """
    def __init__(self, tc, scala_result):
        self._tc = tc
        self._mu = scala_result.mu()
        self._sigma = scala_result.sigma()

    @property
    def mu(self):
        """ (list[float]) The mean vector of the distribution """
        return list(self._tc.jutils.convert.from_scala_seq(self._mu))

    @property
    def sigma(self):
        """ (list[list[float]]) The covariance matrix of the distribution """
        sigma_list = self._tc.jutils.convert.from_scala_seq(self._sigma)
        return [list(self._tc.jutils.convert.from_scala_seq(s)) for s in sigma_list]


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

        >>> model = tc.models.clustering.gmm.train(frame, ["data"], [1.0], 3 ,seed=1)

        >>> model.k
        3

        <skip>
        >>> for g in model.gaussians:
        ...     print g
        mu    = [1.1984786097160265]
        sigma = [[0.5599222134199012]]
        mu    = [6.643997733061858]
        sigma = [[2.19222016401446]]
        mu    = [6.79435719737145]
        sigma = [[2.2637494400157774]]

        </skip>

        <hide>
        >>> expected_mu = [1.1984454608177824,6.6173304476544335,6.79969916638852]
        >>> expected_sigma = [[0.5599200477022921],[2.1848346923369246],[2.2623755196701305]]

        >>> actual_mu = [g.mu[0] for g in model.gaussians]
        >>> actual_sigma = [g.sigma[0] for g in model.gaussians]

        >>> assert(np.allclose(expected_mu, actual_mu, atol=1e+01))
        >>> assert(np.allclose(expected_sigma, actual_sigma, atol=1e+01))
        </hide>

        >>> predicted_frame =  model.predict(frame)

        <skip>
        >>> x = predicted_frame.take(9)
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
        </skip>

        <hide>
        >>> x = predicted_frame.take(9)
        >>> val = set(map(lambda y : y[2], x))
        >>> newlist = [[z[1] for z in x if z[2]==a]for a in val]
        >>> act_out = [[s.encode('ascii') for s in list] for list in newlist]
        >>> act_out.sort(key=lambda x: x[0])
        >>> act_out
        [['ab', 'mn', 'cd', 'gh', 'kl'], ['ij', 'qr', 'ef', 'op']]
        >>> exp_out = [['ij','qr','ef','op'], ['ab','mn','cd','gh','kl']]
        >>> result = False
        >>> for list in act_out:
        ...     if list not in exp_out:
        ...         result = False
        ...     else:
        ...         result = True
        >>> result
        True
        </hide>
        >>> model.observation_columns
        [u'data']

        >>> model.column_scalings
        [1.0]

        >>> model.save("sandbox/gmm")

        >>> restored = tc.load("sandbox/gmm")

        >>> model.cluster_sizes(frame) == restored.cluster_sizes(frame)
        True

    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model


    @staticmethod
    def _from_scala(tc, scala_model):
        """Loads a trained gaussian mixture model from a scala model"""
        return GaussianMixtureModel(tc, scala_model)

    @property
    def observation_columns(self):
        """observation columns used for model training"""
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.observationColumns()))

    @property
    def column_scalings(self):
        """column containing the scalings used for model training"""
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.columnScalings()))

    @property
    def k(self):
        """maximum limit for number of resulting clusters"""
        return self._scala.k()

    @property
    def max_iterations(self):
        """maximum number of iterations"""
        return self._scala.maxIterations()

    @property
    def convergence_tol(self):
        """convergence tolerance"""
        return self._scala.convergenceTol()

    @property
    def seed(self):
        """seed used during training of the model"""
        return self._scala.seed()

    @property
    def gaussians(self):
        """Gaussian object, which contains the mu and sigma values"""
        g = self._tc.jutils.convert.from_scala_seq(self._scala.gaussians())
        results = []
        for i in g:
            results.append(Gaussian(self._tc, i))
        return results

    def cluster_sizes(self, frame):
        """a map of clusters and their sizes"""
        cs = self._scala.computeGmmClusterSize(frame._scala)
        return self._tc.jutils.convert.scala_map_to_python(cs)

    def predict(self, frame, columns=None):
        """method to predict on a given frame"""
        from sparktk.frame.frame import Frame
        c = self.__columns_to_option(columns)
        return Frame(self._tc, self._scala.predict(frame._scala, c))

    def __columns_to_option(self, c):
        if c is not None:
            c = self._tc.jutils.convert.to_scala_vector_string(c)
        return self._tc.jutils.convert.to_scala_option(c)


    def save(self, path):
        """save the trained model to the given path"""
        self._scala.save(self._tc._scala_sc, path)

    def export_to_mar(self, path):
        """ export the trained model to MAR format for Scoring Engine """
        if isinstance(path, basestring):
            return self._scala.exportToMar(self._tc._scala_sc, path)

del PropertiesObject
