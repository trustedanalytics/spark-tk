"""
ARIMA (Autoregressive Integrated Moving Average) Model
"""

from sparktk.loggers import log_load; log_load(__name__); del log_load
from sparktk.propobj import PropertiesObject
from sparktk import TkContext

__all__ = ["train", "load", "ArimaModel"]

def train(ts, p, d, q, include_intercept=True, method="css-cgd", init_params=None, tc=TkContext.implicit):
    """
    Creates Autoregressive Integrated Moving Average (ARIMA) Model from the specified time series values.

    Given a time series, fits an non-seasonal Autoregressive Integrated Moving Average (ARIMA) model of
    order (p, d, q) where p represents the autoregression terms, d represents the order of differencing, and q
    represents the moving average error terms.  If includeIntercept is true, the model is fitted with an intercept.

    Parameters
    ----------

    :param ts: (List[float]) Time series to which to fit an ARIMA(p, d, q) model.
    :param p: (int) Autoregressive order
    :param d: (int) Differencing order
    :param q: (int) Moving average order
    :param include_intercept: (Optional(boolean)) If True, the model is fit with an intercept.  Default is True.
    :param method: (Optional(string)) Objective function and optimization method.  Current options are:
                   'css-bobyqa' and 'css-cgd'.  Both optimize the log likelihood in terms of the conditional
                   sum of squares.  The first uses BOBYQA for optimization, while the second uses conjugate
                   gradient descent.  Default is 'css-cgd'.
    :param init_params: (Optional(List[float]) A set of user provided initial parameters for optimization. If the
                        list is empty (default), initialized using Hannan-Rissanen algorithm. If provided, order
                        of parameter should be: intercept term, AR parameters (in increasing order of lag), MA
                        parameters (in increasing order of lag).
    :return: (ArimaModel) Trained ARIMA model
    """
    if not isinstance(ts, list):
        raise TypeError("'ts' parameter must be a list")
    if not isinstance(p, int):
        raise TypeError("'p' parameter must be an integer.")
    if not isinstance(d, int):
        raise TypeError("'d' parameter must be an integer.")
    if not isinstance(q, int):
        raise TypeError("'q' parameter must be an integer.")
    if not isinstance(include_intercept, bool):
        raise TypeError("'include_intercept' parameter must be a boolean")
    if not isinstance(method, basestring):
        raise TypeError("'method' parameter must be a string")
    if init_params is not None:
        if not isinstance(init_params, list):
            raise TypeError("'init_params' parameter must be a list")
    TkContext.validate(tc)

    _scala_obj = get_scala_obj(tc)
    scala_ts = tc.jutils.convert.to_scala_list_double(ts)
    scala_init_params = tc.jutils.convert.to_scala_option_list_double(init_params)
    scala_model = _scala_obj.train(scala_ts, p, d, q, include_intercept, method, scala_init_params)

    return ArimaModel(tc, scala_model)

def load(path, tc=TkContext.implicit):
    """load ArimaModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, ArimaModel)

def get_scala_obj(tc):
    """Gets reference to the ARIMA model scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.timeseries.arima.ArimaModel


class ArimaModel(PropertiesObject):
    """
    A trained Autoregressive Integrated Moving Average (ARIMA) model.

    Example
    -------

    <hide>
        >>> from sparktk import dtypes
        >>> frame = tc.frame.create([["2015-01-01T00:00:00.000Z","Sarah",12.88969427],["2015-01-02T00:00:00.000Z","Sarah",13.54964408],
        ...                          ["2015-01-03T00:00:00.000Z","Sarah",13.8432745],["2015-01-04T00:00:00.000Z","Sarah",12.13843611],
        ...                          ["2015-01-05T00:00:00.000Z","Sarah",12.81156092],["2015-01-06T00:00:00.000Z","Sarah",14.2499628],
        ...                          ["2015-01-07T00:00:00.000Z","Sarah",15.12102595]],
        ...                          [("timestamp", dtypes.datetime),("name", str),("value", float)])
    </hide>

    Consider the following frame that has three columns: timestamp, name, and value.

        >>> frame.inspect()
        [#]  timestamp                 name   value
        =================================================
        [0]  2015-01-01T00:00:00.000Z  Sarah  12.88969427
        [1]  2015-01-02T00:00:00.000Z  Sarah  13.54964408
        [2]  2015-01-03T00:00:00.000Z  Sarah   13.8432745
        [3]  2015-01-04T00:00:00.000Z  Sarah  12.13843611
        [4]  2015-01-05T00:00:00.000Z  Sarah  12.81156092
        [5]  2015-01-06T00:00:00.000Z  Sarah   14.2499628
        [6]  2015-01-07T00:00:00.000Z  Sarah  15.12102595

    Define the date time index:

        >>> datetimeindex = ['2015-01-01T00:00:00.000Z','2015-01-02T00:00:00.000Z',
        ... '2015-01-03T00:00:00.000Z','2015-01-04T00:00:00.000Z','2015-01-05T00:00:00.000Z',
        ... '2015-01-06T00:00:00.000Z','2015-01-07T00:00:00.000Z']


    Then, create a time series frame from the frame of observations, since the ARIMA model
    expects data to be in a time series format (where the time series values are in a
    vector column).

        >>> ts = frame.timeseries_from_observations(datetimeindex, "timestamp","name","value")
        <progress>

        >>> ts.inspect()
        [#]  name
        ==========
        [0]  Sarah
        <BLANKLINE>
        [#]  value
        ================================================================================
        [0]  [12.88969427, 13.54964408, 13.8432745, 12.13843611, 12.81156092, 14.2499628, 15.12102595]


    Use the frame take function to get one row of data with just the "value" column

        >>> ts_frame_data = ts.take(n=1,offset=0,columns=["value"]).data

    From the ts_frame_data, get the first row and first column to extract out just the time series values.

        >>> ts_values = ts_frame_data[0][0].tolist()

    Train the ARIMA model by specifying the list of time series values, p, d, q (and optionally include_intercept,
    method, and init_params):

        >>> model = tc.models.timeseries.arima.train(ts_values, 1, 0, 1)

    Forecast future values by calling predict().  By default, the number of forecasted values is equal to the number
    of values that was passed to during training.  In this example, we trained with 7 valuse in ts_values, so 7 values
    are returned from predict().

        >>> model.predict()
        [12.674342627141744,
         13.638048984791693,
         13.682219498657313,
         13.883970022400577,
         12.49564914570843,
         13.66340392811346,
         14.201275185574925]

    To forecast more values beyond the length of the time series, specify the number of future_periods to add on.  Here
    we will specify future_periods = 3, so that we get a total of 10 predicted values.

        >>> model.predict(future_periods=3)
        [12.674342627141744,
         13.638048984791693,
         13.682219498657313,
         13.883970022400577,
         12.49564914570843,
         13.66340392811346,
         14.201275185574925,
         14.345159879072785,
         13.950679344897772,
         13.838311126610202]

    Save the trained model to use later:

        >>> save_path = "sandbox/savedArimaModel"
        >>> model.save(save_path)

    The model can be loaded from the tk context like:

        >>> loaded_model = tc.load(save_path)

        >>> loaded_model.predict()
        [12.674342627141744,
         13.638048984791693,
         13.682219498657313,
         13.883970022400577,
         12.49564914570843,
         13.66340392811346,
         14.201275185574925]

    """
    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        """
        Load an ARIMA model

        :param tc: (TkContext) Active TkContext
        :param scala_model: (scala ArimaModel) Scala model to load.
        :return: (ArimaModel) ArimaModel object
        """
        return ArimaModel(tc, scala_model)

    @property
    def ts_values(self):
        """
        List of time series values that were used to fit the model.
        """
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.timeseriesValues()))

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
    def include_intercept(self):
        """
        True, if the model was fit with an intercept.
        """
        return self._scala.includeIntercept()

    @property
    def method(self):
        """
         Objective function and optimization method.  Either: 'css-bobyqa' or 'css-cgd'.
        """
        return self._scala.method()

    @property
    def init_params(self):
        """
        A set of user provided initial parameters for optimization
        """
        return self._scala.initParams()

    @property
    def coefficients(self):
        """
        Coefficient values from the trained model (intercept, AR, MA, with increasing degrees).
        """
        return list(self._tc.jutils.convert.from_scala_seq(self._scala.coefficients()))

    def predict(self, future_periods = 0, ts = None):
        """
        Forecasts future periods using ARIMA.

        Provided fitted values of the time series as 1-step ahead forecasts, based on current model parameters, then
        provide future periods of forecast.  We assume AR terms prior to the start of the series are equal to the
        model's intercept term (or 0.0, if fit without an intercept term).  Meanwhile, MA terms prior to the start
        are assumed to be 0.0.  If there is differencing, the first d terms come from the original series.

        Parameters
        ----------

        :param future_periods: (int) Periods in the future to forecast (beyond length of time series that the
                               model was trained with).
        :param ts: (Optional(List[float])) Optional list of time series values to use as golden values.  If no time
                   series values are provided, the values used during training will be used during forecasting.

        """
        if not isinstance(future_periods, int):
            raise TypeError("'future_periods' parameter must be an integer.")
        if ts is not None:
            if not isinstance(ts, list):
                raise TypeError("'ts' parameter must be a list of float values." )

        ts_predict_values = self._tc.jutils.convert.to_scala_option_list_double(ts)

        return list(self._tc.jutils.convert.from_scala_seq(self._scala.predict(future_periods, ts_predict_values)))

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
        """

        if not isinstance(path, basestring):
            raise TypeError("path parameter must be a str, but received %s" % type(path))

        self._scala.exportToMar(self._tc._scala_sc, path)

del PropertiesObject
