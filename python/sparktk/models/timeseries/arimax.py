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
ARIMAX (Autoregressive Integrated Moving Average with Exogeneous Variables) Model
"""

from sparktk.loggers import log_load; log_load(__name__); del log_load
from sparktk.lazyloader import implicit
from sparktk.propobj import PropertiesObject

def train(frame, ts_column, x_columns, p, d, q, x_max_lag, include_original_x=True, include_intercept=True, init_params=None):
    """
    Creates Autoregressive Integrated Moving Average with Explanatory Variables (ARIMAX) Model from the specified
    time series values.

    Given a time series, fits an non-seasonal Autoregressive Integrated Moving Average with Explanatory Variables
    (ARIMAX) model of order (p, d, q) where p represents the autoregression terms, d represents the order of differencing
    and q represents the moving average error terms. X_max_lag represents the maximum lag order for exogenous variables.
    If include_original_x is true, the model is fitted with an original exogenous variables. If includeIntercept is true,
    the model is fitted with an intercept.

    :param frame: (Frame) Frame used for training.
    :param ts_column: (str) Name of the column that contains the time series values.
    :param x_columns: (List(str)) Names of the column(s) that contain the values of exogenous regressors.
    :param p: (int) Autoregressive order
    :param d: (int) Differencing order
    :param q: (int) Moving average order
    :param x_max_lag: (int) The maximum lag order for exogenous variables.
    :param include_original_x: (Optional(boolean)) If True, the model is fit with an original exogenous variables
                               (intercept for exogenous variables). Default is True.
    :param include_intercept: (Optional(boolean)) If True, the model is fit with an intercept.  Default is True.
    :param init_params: (Optional(List[float]) A set of user provided initial parameters for optimization. If the
                        list is empty (default), initialized using Hannan-Rissanen algorithm. If provided, order
                        of parameter should be: intercept term, AR parameters (in increasing order of lag), MA
                        parameters (in increasing order of lag) and paramteres for exogenous variables (in
                        increasing order of lag).
    :return: (ArimaxModel) Trained ARIMAX model
    """

    if not isinstance(ts_column, basestring):
        raise TypeError("'ts_column' should be a string (name of the column that has the timeseries value).")
    if not isinstance(x_columns, list) or not all(isinstance(c, str) for c in x_columns):
        raise TypeError("'x_columns' should be a list of strings (names of the exogenous columns).")
    elif len(x_columns) <= 0:
        raise ValueError("'x_columns' should not be empty.")
    if not isinstance(p, int):
        raise TypeError("'p' parameter must be an integer.")
    if not isinstance(d, int):
        raise TypeError("'d' parameter must be an integer.")
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

    tc = frame._tc
    _scala_obj = _get_scala_obj(tc)
    scala_x_columns = tc.jutils.convert.to_scala_vector_string(x_columns)
    scala_init_params = tc.jutils.convert.to_scala_option_list_double(init_params)
    scala_model = _scala_obj.train(frame._scala, ts_column, scala_x_columns, p, d, q, x_max_lag, include_original_x, include_intercept, scala_init_params)

    return ArimaxModel(tc, scala_model)

def load(path, tc=implicit):
    """load ARIMAXModel from given path"""
    if tc is implicit:
        implicit.error("tc")
    return tc.load(path, ArimaxModel)

def _get_scala_obj(tc):
    """Gets reference to the ARIMAX model scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.timeseries.arimax.ArimaxModel


class ArimaxModel(PropertiesObject):
    """
    A trained ARIMAX model.

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
        ...                          [2.7, 1.6, 553.0, 10.5],
        ...                          [1.1, 3.2, 667.0, 10.2],
        ...                          [2.0, 8.0, 900.0, 10.8],
        ...                          [2.2, 9.5, 960.0, 10.5],
        ...                          [2.7, 6.3, 827.0, 10.8],
        ...                          [2.5, 5.0, 762.0, 10.5],
        ...                          [2.6, 5.2, 774.0, 9.5],
        ...                          [2.9, 7.3, 869.0, 8.3]],
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


        >>> model = tc.models.timeseries.arimax.train(frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"], 1, 1, 1, 1, True, False)
        <progress>

        >>> model.c
        0.24886373113659435

        >>> model.ar
        [-0.8612398115782316]

        >>> model.ma
        [-0.45556700539598505]

        >>> model.xreg
        [0.09496697769170012, -0.00043805552312166737, 0.0006888829627820128, 0.8523170824191132, -0.017901092786057428, 0.017936687425751337]

    In this example, we will call predict using the same frame that was used for training, again specifying the name
    of the time series column and the names of the columns that contain exogenous regressors.

        >>> predicted_frame = model.predict(frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"])
        <progress>

    The predicted_frame that's return has a new column called *predicted_y*.  This column contains the predicted
    time series values.

        >>> predicted_frame.column_names
        [u'CO_GT', u'C6H6_GT', u'PT08_S2_NMHC', u'T', u'predicted_y']

        >>> predicted_frame.inspect(columns=["CO_GT","predicted_y"])
        [#]  CO_GT  predicted_y
        =========================
        [0]    2.6  2.83896716391
        [1]    2.0  2.89056663602
        [2]    2.2  2.84550712171
        [3]    2.2  2.88445194591
        [4]    1.6  2.85091111286
        [5]    1.2   2.8798667019
        [6]    1.2  2.85451566607
        [7]    1.0  2.87634898739
        [8]    2.9  2.85726970866
        [9]    2.6  2.87356376648

    The trained model can be saved to be used later:

        >>> model_path = "sandbox/savedArimaxModel"
        >>> model.save(model_path)

    The saved model can be loaded through the tk context and then used for forecasting values the same way
    that the original model was used.

        >>> loaded_model = tc.load(model_path)

        >>> predicted_frame = loaded_model.predict(frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"])

        >>> predicted_frame.inspect(columns=["CO_GT","predicted_y"])
        [#]  CO_GT  predicted_y
        =========================
        [0]    2.6  2.83896716391
        [1]    2.0  2.89056663602
        [2]    2.2  2.84550712171
        [3]    2.2  2.88445194591
        [4]    1.6  2.85091111286
        [5]    1.2   2.8798667019
        [6]    1.2  2.85451566607
        [7]    1.0  2.87634898739
        [8]    2.9  2.85726970866
        [9]    2.6  2.87356376648

    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, _get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        """
        Load an ARIMAX model

        :param tc: (TkContext) Active TkContext
        :param scala_model: (scala ArimaxModel) Scala model to load.
        :return: (ArimaxModel) ArimaxModel object
        """
        return ArimaxModel(tc, scala_model)

    @property
    def p(self):
        """
        Autoregressive order
        """
        return self._scala.p()

    @property
    def d(self):
        """
        Differencing order
        """
        return self._scala.d()

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

        Predict the time series values for a test frame, based on the specified x values.  Creates a new frame
        revision with the existing columns and a new predicted_y column.

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
        :param path: Path to save
        """
        self._scala.save(self._tc._scala_sc, path)

del PropertiesObject
