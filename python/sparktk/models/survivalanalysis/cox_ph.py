from sparktk.loggers import log_load; log_load(__name__); del log_load

from sparktk.propobj import PropertiesObject
from sparktk import TkContext


__all__ = ["train", "load", "coxPhModel"]

def train(frame,
          time_column,
          covariate_columns,
          censor_column,
          convergence_tolerance=1E-6,
          max_steps=100):
    if frame is None:
        raise ValueError("frame cannot be None")
    if time_column is None:
        raise ValueError("Time column must not be null or empty")
    if censor_column is None:
        raise ValueError("Censor column must not be null or empty")
    if censor_column is None:
        raise ValueError("Censor column must not be null or empty")

    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    scala_observation_columns = tc.jutils.convert.to_scala_vector_string(covariate_columns)

    scala_model = _scala_obj.train(frame._scala,
                                   time_column,
                                   covariate_columns,
                                   censor_column,
                                   convergence_tolerance,
                                   max_steps)
    return SparktkCoxPhModel(tc, scala_model)

def load(path, tc=TkContext.implicit):
    """load LinearRegressionModel from given path"""
    TkContext.validate(tc)
    return tc.load(path, SparktkCoxPhModel)

def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.survivalanalysis.cox_ph.SparktkCoxPhModel

def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.survivalanalysis.cox_ph.SparktkCoxPhModel

class SparktkCoxPhModel(PropertiesObject):

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        return SparktkCoxPhModel(tc, scala_model)

    @property
    def covariate_columns(self):
        """List of column(s) containing the covariate."""
        return self._tc.jutils.convert.from_scala_seq(self._scala.covatiateColumns())

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

    def predict(self, frame, observation_columns=None, comparison_frame=None):
        """
        Predict values for a frame using a trained Linear Regression model

        Parameters
        ----------

        :param frame: (Frame) The frame to predict on
        :param observation_columns: Optional(List[str]) List of column(s) containing the observations
        :param comparison_frame: Optional(Frame) Frame to compare against
        :return: (Frame) returns frame with predicted column added
        """
        c = self.__columns_to_option(observation_columns)
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.predict(frame._scala, c, comparison_frame._scala))

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