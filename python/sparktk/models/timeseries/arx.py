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
ARX (autoregressive exogenous) Model
"""

from sparktk.loggers import log_load; log_load(__name__); del log_load
from sparktk import TkContext

from sparktk.propobj import PropertiesObject

__all__ = ["train", "load", "ArxModel"]

def train(frame, ts_column, x_columns, y_max_lag, x_max_lag, no_intercept=False):
    """
    Creates a ARX model by training on the given frame. Fit an autoregressive model with additional
    exogenous variables.

    Parameters
    ----------

    :param frame: (Frame) Frame used for training
    :param ts_column: (str) Name of the column that contains the time series values.
    :param x_columns: (List(str)) Names of the column(s) that contain the values of exogenous regressors.
    :param y_max_lag: (int) The maximum lag order for the dependent (time series) variable.
    :param x_max_lag: (int) The maximum lag order for exogenous variables.
    :param no_intercept: (bool) A boolean flag indicating if the intercept should be dropped. Default is false.
    :return: (ArxModel) Trained ARX model

    Notes
    -----

    1.  Dataset being trained must be small enough to be worked with on a single node.
    +   If the specified set of exogenous variables is not invertible, an exception is
        thrown stating that the "matrix is singular".  This happens when there are
        certain patterns in the dataset or columns of all zeros.  In order to work
        around the singular matrix issue, try selecting a different set of columns for
        exogenous variables, or use a different time window for training.

    """
    # check parameter/types
    if not isinstance(ts_column, basestring):
        raise TypeError("'ts_column' should be a string (name of the column that has the timeseries value).")
    if not isinstance(x_columns, list) or not all(isinstance(c, str) for c in x_columns):
        raise TypeError("'x_columns' should be a list of strings (names of the exogenous columns).")
    elif len(x_columns) <= 0:
        raise ValueError("'x_columns' should not be empty.")
    if not isinstance(x_max_lag, int):
        raise TypeError("'x_max_lag' should be an integer.")
    if not isinstance(y_max_lag, int):
        raise TypeError("'y_max_lag' should be an integer.")
    if not isinstance(no_intercept, bool):
        raise TypeError("'no_intercept' should be a boolean.")

    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    scala_x_columns = tc.jutils.convert.to_scala_vector_string(x_columns)
    scala_model = _scala_obj.train(frame._scala, ts_column, scala_x_columns, y_max_lag, x_max_lag, no_intercept)

    return ArxModel(tc, scala_model)


def load(path, tc=TkContext.implicit):
    """load ArxModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, ArxModel)


def get_scala_obj(tc):
    """Gets reference to the ArxModel scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.timeseries.arx.ArxModel


class ArxModel(PropertiesObject):
    """
    A trained ARX model.
    
    Example
    -------
    
    Consider the following model trained and tested on the sample data set in *frame* 'frame'.
    The frame has a snippet of air quality data from:

    https://archive.ics.uci.edu/ml/datasets/Air+Quality.

    Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml].
    Irvine, CA: University of California, School of Information and Computer Science.
    
    <hide>
        
        >>> schema = [('Date', str),('Time', str),('CO_GT', float),('PT08_S1_CO', int),('NMHC_GT', int),
        ...           ('C6H6_GT', float),('PT08_S2_NMHC', int),('NOx_GT', int),('PT08_S3_NOx', int),('NO2_GT', int),
        ...           ('PT08_S4_NO2', int),('PT08_S5_O3_', int),('T', float),('RH', float),('AH', float)]

        >>> frame = tc.frame.create([["10/03/2004","18.00.00",2.6,1360,150,11.9,1046,166,1056,113,1692,1268,13.6,48.9,0.7578],
        ...                          ["10/03/2004","19.00.00",2,1292,112,9.4,955,103,1174,92,1559,972,13.3,47.7,0.7255],
        ...                          ["10/03/2004","20.00.00",2.2,1402,88,9.0,939,131,1140,114,1555,1074,11.9,54.0,0.7502],
        ...                          ["10/03/2004","21.00.00",2.2,1376,80,9.2,948,172,1092,122,1584,1203,11.0,60.0,0.7867],
        ...                          ["10/03/2004","22.00.00",1.6,1272,51,6.5,836,131,1205,116,1490,1110,11.2,59.6,0.7888],
        ...                          ["10/03/2004","23.00.00",1.2,1197,38,4.7,750,89,1337,96,1393,949,11.2,59.2,0.7848],
        ...                          ["11/03/2004","00.00.00",1.2,1185,31,3.6,690,62,1462,77,1333,733,11.3,56.8,0.7603],
        ...                          ["11/03/2004","01.00.00",1,1136,31,3.3,672,62,1453,76,1333,730,10.7,60.0,0.7702],
        ...                          ["11/03/2004","02.00.00",0.9,1094,24,2.3,609,45,1579,60,1276,620,10.7,59.7,0.7648],
        ...                          ["11/03/2004","03.00.00",0.6,1010,19,1.7,561,-200,1705,-200,1235,501,10.3,60.2,0.7517],
        ...                          ["11/03/2004","04.00.00",-200,1011,14,1.3,527,21,1818,34,1197,445,10.1,60.5,0.7465],
        ...                          ["11/03/2004","05.00.00",0.7,1066,8,1.1,512,16,1918,28,1182,422,11.0,56.2,0.7366],
        ...                          ["11/03/2004","06.00.00",0.7,1052,16,1.6,553,34,1738,48,1221,472,10.5,58.1,0.7353],
        ...                          ["11/03/2004","07.00.00",1.1,1144,29,3.2,667,98,1490,82,1339,730,10.2,59.6,0.7417],
        ...                          ["11/03/2004","08.00.00",2,1333,64,8.0,900,174,1136,112,1517,1102,10.8,57.4,0.7408],
        ...                          ["11/03/2004","09.00.00",2.2,1351,87,9.5,960,129,1079,101,1583,1028,10.5,60.6,0.7691],
        ...                          ["11/03/2004","10.00.00",1.7,1233,77,6.3,827,112,1218,98,1446,860,10.8,58.4,0.7552],
        ...                          ["11/03/2004","11.00.00",1.5,1179,43,5.0,762,95,1328,92,1362,671,10.5,57.9,0.7352],
        ...                          ["11/03/2004","12.00.00",1.6,1236,61,5.2,774,104,1301,95,1401,664,9.5,66.8,0.7951],
        ...                          ["11/03/2004","13.00.00",1.9,1286,63,7.3,869,146,1162,112,1537,799,8.3,76.4,0.8393],
        ...                          ["11/03/2004","14.00.00",2.9,1371,164,11.5,1034,207,983,128,1730,1037,8.0,81.1,0.8736],
        ...                          ["11/03/2004","15.00.00",2.2,1310,79,8.8,933,184,1082,126,1647,946,8.3,79.8,0.8778],
        ...                          ["11/03/2004","16.00.00",2.2,1292,95,8.3,912,193,1103,131,1591,957,9.7,71.2,0.8569],
        ...                          ["11/03/2004","17.00.00",2.9,1383,150,11.2,1020,243,1008,135,1719,1104,9.8,67.6,0.8185]],
        ...                          schema=schema, validate_schema=True)
        -etc-

    </hide>

        >>> frame.inspect()
        [#]  Date        Time      CO_GT  PT08_S1_CO  NMHC_GT  C6H6_GT  PT08_S2_NMHC
        ============================================================================
        [0]  10/03/2004  18.00.00    2.6        1360      150     11.9          1046
        [1]  10/03/2004  19.00.00    2.0        1292      112      9.4           955
        [2]  10/03/2004  20.00.00    2.2        1402       88      9.0           939
        [3]  10/03/2004  21.00.00    2.2        1376       80      9.2           948
        [4]  10/03/2004  22.00.00    1.6        1272       51      6.5           836
        [5]  10/03/2004  23.00.00    1.2        1197       38      4.7           750
        [6]  11/03/2004  00.00.00    1.2        1185       31      3.6           690
        [7]  11/03/2004  01.00.00    1.0        1136       31      3.3           672
        [8]  11/03/2004  02.00.00    0.9        1094       24      2.3           609
        [9]  11/03/2004  03.00.00    0.6        1010       19      1.7           561
        <BLANKLINE>
        [#]  NOx_GT  PT08_S3_NOx  NO2_GT  PT08_S4_NO2  PT08_S5_O3_  T     RH    AH
        ==============================================================================
        [0]     166         1056     113         1692         1268  13.6  48.9  0.7578
        [1]     103         1174      92         1559          972  13.3  47.7  0.7255
        [2]     131         1140     114         1555         1074  11.9  54.0  0.7502
        [3]     172         1092     122         1584         1203  11.0  60.0  0.7867
        [4]     131         1205     116         1490         1110  11.2  59.6  0.7888
        [5]      89         1337      96         1393          949  11.2  59.2  0.7848
        [6]      62         1462      77         1333          733  11.3  56.8  0.7603
        [7]      62         1453      76         1333          730  10.7  60.0  0.7702
        [8]      45         1579      60         1276          620  10.7  59.7  0.7648
        [9]    -200         1705    -200         1235          501  10.3  60.2  0.7517

    We will be using the column "T" (temperature) as our time series value:

        >>> y = "T"

    The sensor values will be used as our exogenous variables:

        >>> x = ['CO_GT','PT08_S1_CO','NMHC_GT','C6H6_GT','PT08_S2_NMHC','NOx_GT','PT08_S3_NOx','NO2_GT','PT08_S4_NO2','PT08_S5_O3_']

    Train the model and then take a look at the model properties and coefficients:

        >>> model = tc.models.timeseries.arx.train(frame, y, x, 0, 0, True)
        <progress>

        >>> model
        c            = 0.0
        coefficients = [0.005567992923907625, -0.010969068059453009, 0.012556586798371176, -0.39792503380811506, 0.04289162879826746, -0.012253952164677924, 0.01192148525581035, 0.014100699808650077, -0.021091473795935345, 0.007622676727420039]
        no_intercept = True
        x_max_lag    = 0
        y_max_lag    = 0

    In this example, we will call predict using the same frame that was used for training, again specifying the name
    of the time series column and the names of the columns that contain exogenous regressors.

        >>> predicted_frame = model.predict(frame, y, x)
        <progress>

    The predicted_frame that's return has a new column called *predicted_y*.  This column contains the predicted
    time series values.

        >>> predicted_frame.column_names
        [u'Date',
         u'Time',
         u'CO_GT',
         u'PT08_S1_CO',
         u'NMHC_GT',
         u'C6H6_GT',
         u'PT08_S2_NMHC',
         u'NOx_GT',
         u'PT08_S3_NOx',
         u'NO2_GT',
         u'PT08_S4_NO2',
         u'PT08_S5_O3_',
         u'T',
         u'RH',
         u'AH',
         u'predicted_y']

        >>> predicted_frame.inspect(n=15, columns=["T","predicted_y"])
        [##]  T     predicted_y
        =========================
        [0]   13.6   13.236459938
        [1]   13.3  13.0250130899
        [2]   11.9  11.4147282294
        [3]   11.0  11.3157457822
        [4]   11.2  11.3982074883
        [5]   11.2  11.7079198051
        [6]   11.3  10.7879916472
        [7]   10.7   10.527428478
        [8]   10.7  10.4439615476
        [9]   10.3   10.276662138
        [10]  10.1  10.0999996581
        [11]  11.0  11.2849327784
        [12]  10.5  10.5726885589
        [13]  10.2  10.1984619512
        [14]  10.8  11.0063774234


    The trained model can be saved to be used later:

        >>> model_path = "sandbox/savedArxModel"
        >>> model.save(model_path)

    The saved model can be loaded through the tk context and then used for forecasting values the same way
    that the original model was used.

        >>> loaded_model = tc.load(model_path)
        
        >>> predicted_frame = loaded_model.predict(frame, y, x)

        >>> predicted_frame.inspect(n=15,columns=["T","predicted_y"])
        [##]  T     predicted_y
        =========================
        [0]   13.6   13.236459938
        [1]   13.3  13.0250130899
        [2]   11.9  11.4147282294
        [3]   11.0  11.3157457822
        [4]   11.2  11.3982074883
        [5]   11.2  11.7079198051
        [6]   11.3  10.7879916472
        [7]   10.7   10.527428478
        [8]   10.7  10.4439615476
        [9]   10.3   10.276662138
        [10]  10.1  10.0999996581
        [11]  11.0  11.2849327784
        [12]  10.5  10.5726885589
        [13]  10.2  10.1984619512
        [14]  10.8  11.0063774234

    The trained model can also be exported to a .mar file, to be used with the scoring engine:

        >>> canonical_path = model.export_to_mar("sandbox/arx.mar")

    <hide>
        >>> import os
        >>> assert(os.path.isfile(canonical_path))
    </hide>

    """
    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        """
        Load an ARX model

        :param tc: (TkContext) Active TkContext
        :param scala_model: (scala ArxModel) Scala model to load
        :return: (ArxModel) ArxModel object
        """
        return ArxModel(tc, scala_model)

    @property
    def y_max_lag(self):
        """
        The maximum lag order for the dependent (time series) values.
        """
        return self._scala.yMaxLag()


    @property
    def x_max_lag(self):
        """
        The maximum lag order for exogenous variables.
        """
        return self._scala.xMaxLag()

    @property
    def c(self):
        """
        An intercept term (zero if none desired), from the trained model.
        """
        return self._scala.c()

    @property
    def coefficients(self):
        """
        Coefficient values from the trained model.
        """
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.coefficients()))

    @property
    def no_intercept(self):
        """
        A boolean flag indicating if the intercept should be dropped.
        """
        return self._scala.noIntercept()

    def predict(self, frame, ts_column, x_columns):
        """
        New frame with column of predicted y values

        Predict the time series values for a test frame, based on the specified x values.  Creates a new frame
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
        Save the trained model to the specified path.

        Parameters
        ----------
        :param path: (str) Path to save
        """
        self._scala.save(self._tc._scala_sc, path)

    def export_to_mar(self, path):
        """
        Exports the trained model as a model archive (.mar) to the specified path.

        Parameters
        ----------

        :param path: (str) Path to save the trained model
        :returns (str) Full path to the saved .mar file

        """

        if not isinstance(path, basestring):
            raise TypeError("path parameter must be a str, but received %s" % type(path))

        return self._scala.exportToMar(self._tc._scala_sc, path)

del PropertiesObject
