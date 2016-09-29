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

__all__ = ["train", "load", "PcaModel"]

def train(frame, columns, mean_centered=True, k=None):
    """
    Creates a PcaModel by training on the given frame

    Parameters
    ----------

    :param frame: (Frame) A frame of training data.
    :param columns: (str or list[str]) Names of columns containing the observations for training.
    :param mean_centered: (bool) Whether to mean center the columns.
    :param k: (int) Principal component count. Default is the number of observation columns.
    :return: (PcaModel) The trained PCA model
    """
    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    scala_columns = tc.jutils.convert.to_scala_vector_string(columns)
    if not isinstance(mean_centered, bool):
        raise ValueError("mean_centered must be a bool, received %s" % type(mean_centered))
    scala_k = tc.jutils.convert.to_scala_option(k)
    scala_model = _scala_obj.train(frame._scala, scala_columns, mean_centered, scala_k)
    return PcaModel(tc, scala_model)


def load(path, tc=TkContext.implicit):
    """load PcaModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, PcaModel)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.dimreduction.pca.PcaModel


class PcaModel(PropertiesObject):
    """
    Princiapl Component Analysis Model

    Example
    -------

        >>> frame = tc.frame.create([[2.6,1.7,0.3,1.5,0.8,0.7],
        ...                          [3.3,1.8,0.4,0.7,0.9,0.8],
        ...                          [3.5,1.7,0.3,1.7,0.6,0.4],
        ...                          [3.7,1.0,0.5,1.2,0.6,0.3],
        ...                          [1.5,1.2,0.5,1.4,0.6,0.4]],
        ...                         [("1", float), ("2", float), ("3", float), ("4", float), ("5", float), ("6", float)])
        -etc-



        >>> frame.inspect()
        [#]  1    2    3    4    5    6
        =================================
        [0]  2.6  1.7  0.3  1.5  0.8  0.7
        [1]  3.3  1.8  0.4  0.7  0.9  0.8
        [2]  3.5  1.7  0.3  1.7  0.6  0.4
        [3]  3.7  1.0  0.5  1.2  0.6  0.3
        [4]  1.5  1.2  0.5  1.4  0.6  0.4

        >>> model = tc.models.dimreduction.pca.train(frame, ['1','2','3','4','5','6'], mean_centered=True, k=4)

        >>> model.columns
        [u'1', u'2', u'3', u'4', u'5', u'6']

        <skip>
        >>> model.column_means
        [2.92, 1.48, 0.4, 1.3, 0.7, 0.52]
        </skip>

        <hide>
        >>> expected_means =  [2.92, 1.48, 0.4, 1.3, 0.7, 0.52]

        >>> tc.testing.compare_floats(expected_means, model.column_means, precision=0.001)

        </hide>

        <skip>
        >>> model.singular_values
        [1.804817009663242, 0.8835344148403884, 0.7367461843294286, 0.15234027471064396]
        </skip>

        <hide>
        >>> tc.testing.compare_floats([1.804817009663242, 0.8835344148403884, 0.7367461843294286, 0.15234027471064396], model.singular_values, 0.00000000001)

        </hide>

        <skip>
        >>> model.right_singular_vectors
        [[-0.9906468642089336, 0.11801374544146298, 0.02564701035332026, 0.04852509627553534], [-0.07735139793384983, -0.6023104604841426, 0.6064054412059492, -0.4961696216881456], [0.028850639537397756, 0.07268697636708586, -0.24463936400591005, -0.17103491337994484], [0.10576208410025367, 0.5480329468552814, 0.7523059089872701, 0.2866144016081254], [-0.024072151446194616, -0.30472267167437644, -0.011259366445851784, 0.48934541040601887], [-0.00617295395184184, -0.47414707747028795, 0.0753345822621543, 0.6329307498105843]]

        </skip>

        <hide>
        >>> expected = [[-0.9906468642089336, 0.11801374544146298, 0.02564701035332026, 0.04852509627553534], [-0.07735139793384983, -0.6023104604841426, 0.6064054412059492, -0.4961696216881456], [0.028850639537397756, 0.07268697636708586, -0.24463936400591005, -0.17103491337994484], [0.10576208410025367, 0.5480329468552814, 0.7523059089872701, 0.2866144016081254], [-0.024072151446194616, -0.30472267167437644, -0.011259366445851784, 0.48934541040601887], [-0.00617295395184184, -0.47414707747028795, 0.0753345822621543, 0.6329307498105843]]

        >>> got = model.right_singular_vectors

        >>> if len(expected) != len(got):
        ...     raise RuntimeError("Mismatched lengths for right_singular_vectors, expected %s != got %s" % (len(expected), len(got)))

        >>> for i in xrange(len(got)):
        ...     tc.testing.compare_floats(expected[i], got[i], 0.000001)

        </hide>

        >>> model.predict(frame, mean_centered=True, t_squared_index=True, columns=['1','2','3','4','5','6'], k=3)
        -etc-

        >>> frame.inspect()
        [#]  1    2    3    4    5    6    p_1              p_2
        ===================================================================
        [0]  2.6  1.7  0.3  1.5  0.8  0.7   0.314738695012  -0.183753549226
        [1]  3.3  1.8  0.4  0.7  0.9  0.8  -0.471198363594  -0.670419608227
        [2]  3.5  1.7  0.3  1.7  0.6  0.4  -0.549024749481   0.235254068619
        [3]  3.7  1.0  0.5  1.2  0.6  0.3  -0.739501762517   0.468409769639
        [4]  1.5  1.2  0.5  1.4  0.6  0.4    1.44498618058   0.150509319195
        <BLANKLINE>
        [#]  p_3              t_squared_index
        =====================================
        [0]   0.312561560113   0.253649649849
        [1]  -0.228746130528   0.740327252782
        [2]   0.465756549839   0.563086507007
        [3]  -0.386212142456   0.723748467549
        [4]  -0.163359836968   0.719188122813

        >>> model.save('sandbox/pca1')

        >>> model2 = tc.load('sandbox/pca1')

        >>> model2.k
        4

    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        return PcaModel(tc, scala_model)

    @property
    def columns(self):
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.columns()))

    @property
    def mean_centered(self):
        return self._scala.meanCentered()

    @property
    def k(self):
        return self._scala.k()

    @property
    def column_means(self):
        return list(self._scala.columnMeansAsArray())

    @property
    def singular_values(self):
        return list(self._scala.singularValuesAsArray())

    @property
    def right_singular_vectors(self):
        x = list(self._scala.rightSingularVectorsAsArray())
        return [x[i:i+self.k] for i in xrange(0,len(x),self.k)]

    def predict(self, frame, columns=None, mean_centered=None, k=None, t_squared_index=False):
        """Adds columns to the given frame which are the principal compenent predictions"""
        if mean_centered is None:
            mean_centered = self.mean_centered
        self._scala.predict(frame._scala,
                            self._tc.jutils.convert.to_scala_option_list_string(columns),
                            mean_centered,
                            self._tc.jutils.convert.to_scala_option(k),
                            t_squared_index)

    def save(self, path):
        self._scala.save(self._tc._scala_sc, path)

    def export_to_mar(self, path):
        self._scala.exportToMar(self._tc._scala_sc, path)