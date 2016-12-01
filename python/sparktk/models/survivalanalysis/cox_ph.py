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

from sparktk.arguments import affirm_type, require_type
from sparktk.propobj import PropertiesObject
from sparktk import TkContext

__all__ = ["train", "load", "CoxProportionalHazardsModel"]

def train(frame,
          time_column,
          covariate_columns,
          censor_column,
          convergence_tolerance=1E-6,
          max_steps=100):
    """
    Creates a CoxProportionalHazardsModel by training on the given frame

    Parameters
    ----------

    :param frame: (Frame) A frame to train the model on
    :param time_column: (str) Column name containing the time of occurence of each observation.
    :param covariate_columns: (Seq[str]) List of column(s) containing the covariates.
    :param censor_column: (str) Column name containing censor value of each observation.
    :param convergence_tolerance: (float) Parameter for the convergence tolerance for iterative algorithms. Default is 1E-6
    :param max_steps: (int) Parameter for maximum number of steps. Default is 100
    :return: (CoxProportionalHazardsModel) A trained coxPh model
    """
    from sparktk.frame.frame import Frame
    require_type(Frame, frame, "frame cannot be None")
    require_type.non_empty_str(time_column, "time_column")
    require_type.non_empty_str(censor_column, "censor_column")
    require_type(float, convergence_tolerance, "convergence_tolerance should be float")
    require_type.non_negative_int(max_steps, "max_steps")
    affirm_type.list_of_str(covariate_columns, "covariate_columns")

    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    scala_covariate_columns = tc.jutils.convert.to_scala_vector_string(covariate_columns)

    scala_model = _scala_obj.train(frame._scala,
                                   time_column,
                                   scala_covariate_columns,
                                   censor_column,
                                   convergence_tolerance,
                                   max_steps)
    return CoxProportionalHazardsModel(tc, scala_model)


def load(path, tc=TkContext.implicit):
    """load CoxProportionalHazardsModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, CoxProportionalHazardsModel)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.survivalanalysis.cox_ph.CoxProportionalHazardsModel


class CoxProportionalHazardsModel(PropertiesObject):
    """
    A trained CoxProportionalHazardsModel

    Example
    -------
        >>> data = [[18,42, 6, 1], [19, 79, 5, 1], [6, 46, 4, 1],[4, 66, 3, 1], [0, 90, 2, 1], [12, 20, 1, 1], [0, 73, 0, 1]]
        >>> frame = tc.frame.create(data, schema=[("x1", int), ("x2", int), ("time", int), ("censor", int)])

        >>> frame.inspect()
        [#]  x1  x2  time  censor
        =========================
        [0]  18  42     6       1
        [1]  19  79     5       1
        [2]   6  46     4       1
        [3]   4  66     3       1
        [4]   0  90     2       1
        [5]  12  20     1       1
        [6]   0  73     0       1

        >>> model = tc.models.survivalanalysis.cox_ph.train(frame, "time", ["x1", "x2"], "censor")

        >>> model
        beta                  = [-0.19214283727219952, -0.00701223703811671]
        censor_column         = censor
        convergence_tolerance = 1e-06
        covariate_columns     = [u'x1', u'x2']
        max_steps             = 100
        mean                  = [8.428571428571429, 59.42857142857143]
        time_column           = time

        >>> predicted_frame = model.predict(frame)

        >>> predicted_frame.inspect()
        [#]  x1  x2  time  censor  hazard_ratio
        =========================================
        [0]  18  42     6       1  0.179627832028
        [1]  19  79     5       1  0.114353154098
        [2]   6  46     4       1   1.75206822111
        [3]   4  66     3       1   2.23633388037
        [4]   0  90     2       1   4.07599759247
        [5]  12  20     1       1  0.663821540526
        [6]   0  73     0       1    4.5920362555

        >>> model.save("sandbox/cox_ph_model")

        >>> restored = tc.load("sandbox/cox_ph_model")

        >>> restored.max_steps == 100
        True

    The trained model can also be exported to a .mar file, to be used with the scoring engine:

        >>> canonical_path = model.export_to_mar("sandbox/cox_ph_model.mar")

    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        return CoxProportionalHazardsModel(tc, scala_model)

    @property
    def covariate_columns(self):
        """List of column(s) containing the covariate."""
        return self._tc.jutils.convert.from_scala_seq(self._scala.covariateColumns())

    @property
    def time_column(self):
        """Column name containing the time for each observation."""
        return self._scala.timeColumn()

    @property
    def censor_column(self):
        """Column name containing the censor value for each observation."""
        return self._scala.censorColumn()

    @property
    def convergence_tolerance(self):
        """Column name containing convergence tolerance."""
        return self._scala.convergenceTolerance()

    @property
    def max_steps(self):
        """The number of training steps until termination"""
        return self._scala.maxSteps()

    @property
    def mean(self):
        """Mean of each column"""
        return  self._tc.jutils.convert.from_scala_seq(self._scala.mean())

    @property
    def beta(self):
        """Trained beta values for each covariate"""
        return  self._tc.jutils.convert.from_scala_seq(self._scala.beta())

    def predict(self, frame, observation_columns=None, comparison_frame=None):
        """
        Predict values for a frame using a trained CoxPH model

        Parameters
        ----------

        :param frame: (Frame) The frame to predict on
        :param observation_columns: Optional(List[str]) List of column(s) containing the observations. Default is list of covariate columns
        :param comparison_frame: Optional(Frame) Frame to compare against. Default is the training frame
        :return: (Frame) returns frame with predicted column added
        """
        observation_columns = self.__columns_to_option(observation_columns)
        comparison_frame = self.__frame_to_option(comparison_frame)
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.predict(frame._scala, observation_columns, comparison_frame))

    def __columns_to_option(self, c):
        if c is not None:
            c = self._tc.jutils.convert.to_scala_list_string(c)
        return self._tc.jutils.convert.to_scala_option(c)

    def __frame_to_option(self, f):
        if f is not None:
            f = f._scala
        return self._tc.jutils.convert.to_scala_option(f)

    def save(self, path):
        """
        Saves the model to given path

        Parameters
        ----------

        :param path: (str) path to save
        """

        self._scala.save(self._tc._scala_sc, path, False)


    def export_to_mar(self, path):
        """
        Exports the trained model as a model archive (.mar) to the specified path.

        Parameters
        ----------

        :param path: (str) Path to save the trained model
        :return: (str) Full path to the saved .mar file

        """

        if not isinstance(path, basestring):
            raise TypeError("path parameter must be a str, but received %s" % type(path))

        return self._scala.exportToMar(self._tc._scala_sc, path)
