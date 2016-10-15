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

"""
MAx (Moving Average with Exogeneous Variables) Model
"""
import unittest

from sparktk.loggers import log_load; log_load(__name__); del log_load
from sparktk.lazyloader import implicit
from sparktk.propobj import PropertiesObject

def train(frame, ts_column, x_columns, q, x_max_lag, include_original_x=True, include_intercept=True, init_params=None):
    """
    Creates Moving Average with Explanatory Variables (MAX) Model from the specified time series values.

    Given a time series, fits Moving Average with Explanatory Variables (MAX) model. Q represents the moving average error
    terms, x_max_lag represents the maximum lag order for exogenous variables. If include_original_x is true, the model is
    fitted with an original exogenous variables. If includeIntercept is true, the model is fitted with an intercept.

    Parameters
    ----------

    :param frame: (Frame) Frame used for training.
    :param ts_column: (str) Name of the column that contains the time series values.
    :param x_columns: (List(str)) Names of the column(s) that contain the values of exogenous regressors.
    :param q: (int) Moving average order
    :param x_max_lag: (int) The maximum lag order for exogenous variables.
    :param include_original_x: (Optional(boolean)) If True, the model is fit with an original exogenous variables
                               (intercept for exogenous variables). Default is True.
    :param include_intercept: (Optional(boolean)) If True, the model is fit with an intercept.  Default is True.
    :param init_params: (Optional(List[float]) A set of user provided initial parameters for optimization. If the
                        list is empty (default), initialized using Hannan-Rissanen algorithm. If provided, order
                        of parameter should be: intercept term (mostly 0), MA parameters (in increasing order of lag)
                        and paramteres for exogenous variables (in increasing order of lag).
    :return: (MaxModel) Trained MAX model
    """

    if not isinstance(ts_column, basestring):
        raise TypeError("'ts_column' should be a string (name of the column that has the timeseries value).")
    if not isinstance(x_columns, list) or not all(isinstance(c, str) for c in x_columns):
        raise TypeError("'x_columns' should be a list of strings (names of the exogenous columns).")
    elif len(x_columns) <= 0:
        raise ValueError("'x_columns' should not be empty.")
    if not isinstance(q, int):
        raise TypeError("'q' parameter must be an integer.")
    if not isinstance(x_max_lag, int):
        raise TypeError("'x_max_lag' should be an integer.")
    if not isinstance(include_original_x, bool):
        raise TypeError("'include_original_x' parameter must be a boolean")
    if not isinstance(include_intercept, bool):
        raise TypeError("'include_intercept' parameter must be a boolean")
    if init_params is not None:
        if not isinstance(init_params, list):
            raise TypeError("'init_params' parameter must be a list")
    if frame is None:
        raise ValueError("'frame' must not be None")

    tc = frame._tc
    _scala_obj = _get_scala_obj(tc)
    scala_x_columns = tc.jutils.convert.to_scala_vector_string(x_columns)
    scala_init_params = tc.jutils.convert.to_scala_option_list_double(init_params)
    scala_model = _scala_obj.train(frame._scala, ts_column, scala_x_columns, q, x_max_lag, include_original_x, include_intercept, scala_init_params)

    return MaxModel(tc, scala_model)

def load(path, tc=implicit):
    """load MaxModel from given path"""
    if tc is implicit:
        implicit.error("tc")
    return tc.load(path, MaxModel)

def _get_scala_obj(tc):
    """Gets reference to the MAX model scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.timeseries.max.MaxModel


class MaxModel(PropertiesObject):
    """
    A trained MAX model.

    Example
    -------
    Data from Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml].
    Irvine, CA: University of California, School of Information and Computer Science.

    Consider the following model trained and tested on the sample data set in *frame* 'frame'.
    The frame has five columns where "CO_GT" is the time series value and "C6H6_GT", "PT08_S2_NMHC"
    and "T" are exogenous inputs.

    CO_GT - True hourly averaged concentration CO in mg/m^3
    C6H6_GT - True hourly averaged Benzene concentration in microg/m^3
    PT08_S2_NMHC - Titania hourly averaged sensor response (nominally NMHC targeted)
    T - Temperature in C

    <hide>
        >>> schema = [("CO_GT", float),("C6H6_GT", float),("PT08_S2_NMHC", float),("T", float)]
        >>> frame = tc.frame.create([[2.6, 11.9, 1046.0, 13.6],
        ...                          [2.0, 9.4, 955.0, 13.3],
        ...                          [2.2, 9.0, 939.0, 11.9],
        ...                          [2.2, 9.2, 948.0, 11.0],
        ...                          [1.6, 6.5, 836.0, 11.2],
        ...                          [1.2, 4.7, 750.0, 11.2],
        ...                          [1.2, 3.6, 690.0, 11.3],
        ...                          [1.0, 3.3, 672.0, 10.7],
        ...                          [2.9, 2.3, 609.0, 10.7],
        ...                          [2.6, 1.7, 561.0, 10.3],
        ...                          [2.0, 1.3, 527.0, 10.1],
        ...                          [2.7, 1.1, 512.0, 11.0],
        ...                          [2.7, 1.6, 553.0, 10.5]],
        ...                          schema=schema, validate_schema=True)
        -etc-

    </hide>

        >>> frame.inspect()
        [#]  CO_GT  C6H6_GT  PT08_S2_NMHC  T
        =======================================
        [0]    2.6     11.9        1046.0  13.6
        [1]    2.0      9.4         955.0  13.3
        [2]    2.2      9.0         939.0  11.9
        [3]    2.2      9.2         948.0  11.0
        [4]    1.6      6.5         836.0  11.2
        [5]    1.2      4.7         750.0  11.2
        [6]    1.2      3.6         690.0  11.3
        [7]    1.0      3.3         672.0  10.7
        [8]    2.9      2.3         609.0  10.7
        [9]    2.6      1.7         561.0  10.3


        >>> model = tc.models.timeseries.max.train(frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"], 2, 1)
        <progress>

        >>> model.ma
        [0.5777638449118448, -0.06530007715221572]

        >>> model.xreg
        [-0.021849032465107086, 0.0009772982251014968, 0.028419655845061332, 1.329220909234935, -0.026697271514035982, -0.099174926201381]

    In this example, we will call predict using the same frame that was used for training, again specifying the name
    of the time series column and the names of the columns that contain exogenous regressors.

        >>> predicted_frame = model.predict(frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"])
        <progress>

    The predicted_frame that's return has a new column called *predicted_y*.  This column contains the predicted
    time series values.

        >>> predicted_frame.column_names
        [u'CO_GT', u'C6H6_GT', u'PT08_S2_NMHC', u'T', u'predicted_y']

    <skip>
        >>> predicted_frame.inspect(columns=["CO_GT","predicted_y"])
        [#]  CO_GT  predicted_y
        =========================
        [0]    2.6  2.61411194003
        [1]    2.0  2.41046087551
        [2]    2.2  2.39156069744
        [3]    2.2  2.36598300718
        [4]    1.6  2.37166693834
        [5]    1.2  2.37166693834
        [6]    1.2  2.37450890393
        [7]    1.0  2.35745711042
        [8]    2.9  2.35745711042
        [9]    2.6  2.34608924808
    </skip>

    <hide>
        >>> results = predicted_frame.take(5, columns=["predicted_y"])

        >>> guess = [2.73574878613, 2.32499522807, 2.37166693834, 2.37166693834, 2.37450890393]

        >>> tc.testing.compare_floats(sum(results, []), guess, precision=0.15)

    </hide>

    The trained model can be saved to be used later:

        >>> model_path = "sandbox/savedMaxModel"

        >>> model.save(model_path)

    The saved model can be loaded through the tk context and then used for forecasting values the same way
    that the original model was used.

        >>> loaded_model = tc.load(model_path)

        >>> predicted_frame = loaded_model.predict(frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"])

    <skip>
        >>> predicted_frame.inspect(columns=["CO_GT","predicted_y"])
        [#]  CO_GT  predicted_y
        =========================
        [0]    2.6  2.61411194003
        [1]    2.0  2.41046087551
        [2]    2.2  2.39156069744
        [3]    2.2  2.36598300718
        [4]    1.6  2.37166693834
        [5]    1.2  2.37166693834
        [6]    1.2  2.37450890393
        [7]    1.0  2.35745711042
        [8]    2.9  2.35745711042
        [9]    2.6  2.34608924808
    </skip>

    The trained model can also be exported to a .mar file, to be used with the scoring engine:

        >>> canonical_path = model.export_to_mar("sandbox/max.mar")

    <hide>
        >>> import os
        >>> assert(os.path.isfile(canonical_path))
    </hide>

    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, _get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        """
        Load an MAX model

        :param tc: (TkContext) Active TkContext
        :param scala_model: (scala MaxModel) Scala model to load.
        :return: (MaxModel) MaxModel object
        """
        return MaxModel(tc, scala_model)

    @property
    def q(self):
        """
        Moving average order
        """
        return self._scala.q()

    @property
    def x_max_lag(self):
        """
        The maximum lag order for exogenous variables.
        """
        return self._scala.xregMaxLag()

    @property
    def includeIntercept(self):
        """
        A boolean flag indicating if the intercept should be included.
        """
        return self._scala.includeIntercept()

    @property
    def includeOriginalXreg(self):
        """
        A boolean flag indicating if the non-lagged exogenous variables should be included.
        """
        return self._scala.includeOriginalXreg()

    @property
    def init_params(self):
        """
        A set of user provided initial parameters for optimization
        """
        return self._scala.initParams()

    @property
    def c(self):
        """
        Intercept
        """
        return self._scala.c()

    @property
    def ar(self):
        """
        Coefficient values from the trained model (AR with increasing degrees).
        """
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.ar()))

    @property
    def ma(self):
        """
        Coefficient values from the trained model (MA with increasing degrees).
        """
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.ma()))

    @property
    def xreg(self):
        """
        Coefficient values from the trained model fox exogenous variables with increasing degrees.
        """
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.xreg()))


    def predict(self, frame, ts_column, x_columns):
        """
        New frame with column of predicted y values

        Predict the time series values for a test frame, based on the specified x values. Creates a new frame
        revision with the existing columns and a new predicted_y column.

        Parameters
        ----------

        :param frame: (Frame) Frame used for predicting the ts values
        :param ts_column: (str) Name of the time series column
        :param x_columns: (List[str]) Names of the column(s) that contain the values of the exogenous inputs.
        :return: (Frame) A new frame containing the original frame's columns and a column *predictied_y*
        """
        if not isinstance(frame, self._tc.frame.Frame):
            raise TypeError("'frame' parameter should be a spark-tk Frame object.")
        if not isinstance(ts_column, basestring):
            raise TypeError("'ts_column' parameter should be a string (name of the column that has the timeseries value).")
        if not isinstance(x_columns, list) or not all(isinstance(c, str) for c in x_columns):
            raise TypeError("'x_columns' parameter should be a list of strings (names of the exogenous columns).")
        elif len(x_columns) <= 0:
            raise ValueError("'x_columns' should not be empty.")
        scala_x_columns = self._tc.jutils.convert.to_scala_vector_string(x_columns)
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.predict(frame._scala, ts_column, scala_x_columns))


    def save(self, path):
        """
        Save the trained model to the specified path

        Parameters
        ----------

        :param path: Path to save
        """
        self._scala.save(self._tc._scala_sc, path)

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

del PropertiesObject
