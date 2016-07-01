"""
ARX (autoregressive exogenous) Model
"""

from sparktk.loggers import log_load; log_load(__name__); del log_load

from sparktk.propobj import PropertiesObject

def train(frame, ts_column, x_columns, y_max_lag, x_max_lag, no_intercept=False):
    """
    Creates a ARX model by training on the given frame. Fit an autoregressive model with additional
    exogenous variables.

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
    _scala_obj = _get_scala_obj(tc)
    scala_x_columns = tc.jutils.convert.to_scala_vector_string(x_columns)
    scala_model = _scala_obj.train(frame._scala, ts_column, scala_x_columns, x_max_lag, y_max_lag, no_intercept)

    return ArxModel(tc, scala_model)

def _get_scala_obj(tc):
    """Gets reference to the ArxModel scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.timeseries.arx.ArxModel

class ArxModel(PropertiesObject):
    """
    A trained ARX model.
    
    Example
    -------
    
    Consider the following model trained and tested on the sample data set in *frame* 'frame'.
    The frame has five columns where "y" is the time series value and "vistors", "wkends",
    "incidentRate", and "seasonality" are exogenous inputs.
    
    <hide>
        >>> schema = [("y", float),("visitors", float),("wkends", float),("incidentRate", float),("seasonality", float)]
        >>> frame = tc.frame.create([[68,278,0,28,0.015132758079119],
        ...                          [89,324,0,28,0.0115112433251418],
        ...                          [96,318,0,28,0.0190129524583803],
        ...                          [98,347,0,28,0.0292307976571017],
        ...                          [70,345,1,28,0.0232811662755677],
        ...                          [88,335,1,29,0.0306535355961641],
        ...                          [76,309,0,29,0.0278080597180392],
        ...                          [104,318,0,29,0.0305241957835221],
        ...                          [64,308,0,29,0.0247039042146302],
        ...                          [89,320,0,29,0.0269026810295449],
        ...                          [76,292,0,29,0.0283254189686074],
        ...                          [66,295,1,29,0.0230224866502836],
        ...                          [84,383,1,21,0.0279373995306813],
        ...                          [49,237,0,21,0.0263853217789767],
        ...                          [47,210,0,21,0.0230224866502836]],
        ...                          schema=schema, validate_schema=True)
        -etc-

    </hide>

        >>> frame.inspect()
        [#]  y      visitors  wkends  incidentRate  seasonality
        ===========================================================
        [0]   68.0     278.0     0.0          28.0  0.0151327580791
        [1]   89.0     324.0     0.0          28.0  0.0115112433251
        [2]   96.0     318.0     0.0          28.0  0.0190129524584
        [3]   98.0     347.0     0.0          28.0  0.0292307976571
        [4]   70.0     345.0     1.0          28.0  0.0232811662756
        [5]   88.0     335.0     1.0          29.0  0.0306535355962
        [6]   76.0     309.0     0.0          29.0   0.027808059718
        [7]  104.0     318.0     0.0          29.0  0.0305241957835
        [8]   64.0     308.0     0.0          29.0  0.0247039042146
        [9]   89.0     320.0     0.0          29.0  0.0269026810295

        >>> model = tc.models.timeseries.arx.train(frame, "y", ["visitors", "wkends", "incidentRate", "seasonality"], 0, 0, True)
        <progress>

        >>> model.c
        0.0

        >>> model.coefficients
        [0.27583285049358186, -13.096710518563603, -0.030872283789462572, -103.8264674349643]

    In this example, we will call predict using the same frame that was used for training, again specifying the name
    of the time series column and the names of the columns that contain exogenous regressors.

        >>> predicted_frame = model.predict(frame, "y", ["visitors", "wkends", "incidentRate", "seasonality"])
        <progress>

    The predicted_frame that's return has a new column called *predicted_y*.  This column contains the predicted
    time series values.

        >>> predicted_frame.column_names
        [u'y', u'visitors', u'wkends', u'incidentRate', u'seasonality', u'predicted_y']

        >>> predicted_frame.inspect(columns=("y","predicted_y"))
        [#]  y      predicted_y
        =========================
        [0]   68.0  74.2459276772
        [1]   89.0  87.3102478836
        [2]   96.0  84.8763748216
        [3]   98.0  91.8146447141
        [4]   70.0  78.7839977035
        [5]   88.0  75.2293498516
        [6]   76.0  81.4498419659
        [7]  104.0  83.6503308076
        [8]   64.0  81.4963026157
        [9]   89.0  84.5780055922

    The trained model can be saved to be used later:

        >>> model_path = "sandbox/savedArxModel"
        >>> model.save(model_path)

    The saved model can be loaded through the tk context and then used for forecasting values the same way
    that the original model was used.

        >>> loaded_model = tc.load(model_path)
        
        >>> predicted_frame = loaded_model.predict(frame, "y", ["visitors", "wkends", "incidentRate", "seasonality"])

        >>> predicted_frame.inspect(columns=("y","predicted_y"))
        [#]  y      predicted_y
        =========================
        [0]   68.0  74.2459276772
        [1]   89.0  87.3102478836
        [2]   96.0  84.8763748216
        [3]   98.0  91.8146447141
        [4]   70.0  78.7839977035
        [5]   88.0  75.2293498516
        [6]   76.0  81.4498419659
        [7]  104.0  83.6503308076
        [8]   64.0  81.4963026157
        [9]   89.0  84.5780055922

    """
    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, _get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def load(tc, scala_model):
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
        return self._tc.frame.create(self._scala.predict(frame._scala, ts_column, scala_x_columns))

    def save(self, path):
        """
        Save the trained model to the specified path.
        :param path: (str) Path to save
        """
        self._scala.save(self._tc._scala_sc, path)

del PropertiesObject
