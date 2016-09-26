from sparktk.frame.ops.classification_metrics_value import ClassificationMetricsValue
from sparktk.models.logistic_regression_summary_table import LogisticRegressionSummaryTable
from sparktk.loggers import log_load
from sparktk.propobj import PropertiesObject

log_load(__name__)
del log_load

__all__ = ["train", "LogisticRegressionModel"]

def train(frame,
          observation_columns,
          label_column,
          frequency_column=None,
          num_classes=2,
          optimizer="LBFGS",
          compute_covariance=True,
          intercept=True,
          feature_scaling=False,
          threshold=0.5,
          reg_type="L2",
          reg_param=0.0,
          num_iterations=100,
          convergence_tolerance=0.0001,
          num_corrections=10,
          mini_batch_fraction=1.0,
          step_size=1.0):
    """
     Build logistic regression model.

     Creating a Logistic Regression Model using the observation column and label column of the train frame.

    Parameters
    ----------

    :param frame: (Frame) A frame to train the model on.
    :param observation_columns: (List[str]) Column(s) containing the observations.
    :param label_column: (str) Column name containing the label for each observation.
    :param frequency_column:(Option[str]) Optional column containing the frequency of observations.
    :param num_classes: (int) Number of classes
    :param optimizer: (str) Set type of optimizer.
                           LBFGS - Limited-memory BFGS.
                           LBFGS supports multinomial logistic regression.
                           SGD - Stochastic Gradient Descent.
                           SGD only supports binary logistic regression.
    :param compute_covariance: (bool) Compute covariance matrix for the model.
    :param intercept: (bool) Add intercept column to training data.
    :param feature_scaling: (bool) Perform feature scaling before training model.
    :param threshold: (double) Threshold for separating positive predictions from negative predictions.
    :param reg_type: (str) Set type of regularization
                            L1 - L1 regularization with sum of absolute values of coefficients
                            L2 - L2 regularization with sum of squares of coefficients
    :param reg_param: (double) Regularization parameter
    :param num_iterations: (int) Maximum number of iterations
    :param convergence_tolerance: (double) Convergence tolerance of iterations for L-BFGS. Smaller value will lead to higher accuracy with the cost of more iterations.
    :param num_corrections: (int) Number of corrections used in LBFGS update.
                           Default is 10.
                           Values of less than 3 are not recommended;
                           large values will result in excessive computing time.
    :param mini_batch_fraction: (double) Fraction of data to be used for each SGD iteration
    :param step_size: (double) Initial step size for SGD. In subsequent steps, the step size decreases by stepSize/sqrt(t)
    :return A LogisticRegressionModel with a summary of the trained model.
                     The data returned is composed of multiple components\:
                     **int** : *numFeatures*
                         Number of features in the training data
                     **int** : *numClasses*
                         Number of classes in the training data
                     **table** : *summaryTable*
                         A summary table composed of:
                     **Frame** : *CovarianceMatrix (optional)*
                         Covariance matrix of the trained model.
                     The covariance matrix is the inverse of the Hessian matrix for the trained model.
                     The Hessian matrix is the second-order partial derivatives of the model's log-likelihood function.
    """
    tc = frame._tc
    _scala_obj = get_scala_obj(tc)

    if isinstance(observation_columns, basestring):
        observation_columns = [observation_columns]

    scala_observation_columns = tc.jutils.convert.to_scala_vector_string(observation_columns)
    scala_frequency_column = tc.jutils.convert.to_scala_option(frequency_column)

    if not isinstance(compute_covariance, bool):
        raise ValueError("compute_covariance must be a bool, received %s" % type(compute_covariance))
    if not isinstance(intercept, bool):
        raise ValueError("intercept must be a bool, received %s" % type(intercept))
    if not isinstance(feature_scaling, bool):
        raise ValueError("feature_scaling must be a bool, received %s" % type(feature_scaling))

    scala_model = _scala_obj.train(frame._scala,
                                   scala_observation_columns,
                                   label_column,
                                   scala_frequency_column,
                                   num_classes,
                                   optimizer,
                                   compute_covariance,
                                   intercept,
                                   feature_scaling,
                                   threshold,
                                   reg_type,
                                   reg_param,
                                   num_iterations,
                                   convergence_tolerance,
                                   num_corrections,
                                   mini_batch_fraction,
                                   float(step_size))

    return LogisticRegressionModel(tc, scala_model)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.classification.logistic_regression.LogisticRegressionModel


class LogisticRegressionModel(PropertiesObject):
    """
    A trained logistic regression model

    Example
    --------

        >>> rows = [[4.9,1.4,0], [4.7,1.3,0], [4.6,1.5,0], [6.3,4.9,1],[6.1,4.7,1], [6.4,4.3,1], [6.6,4.4,1],[7.2,6.0,2], [7.2,5.8,2], [7.4,6.1,2], [7.9,6.4,2]]
        >>> schema = [('Sepal_Length', float),('Petal_Length', float), ('Class', int)]
        >>> frame = tc.frame.create(rows, schema)
        <progress>

    Consider the following frame containing three columns.

        >>> frame.inspect()
        [#]  Sepal_Length  Petal_Length  Class
        ======================================
        [0]           4.9           1.4      0
        [1]           4.7           1.3      0
        [2]           4.6           1.5      0
        [3]           6.3           4.9      1
        [4]           6.1           4.7      1
        [5]           6.4           4.3      1
        [6]           6.6           4.4      1
        [7]           7.2           6.0      2
        [8]           7.2           5.8      2
        [9]           7.4           6.1      2

        >>> model = tc.models.classification.logistic_regression.train(frame, ['Sepal_Length', 'Petal_Length'], 'Class', num_classes=3, optimizer='LBFGS', compute_covariance=True)
        <progress>

        <skip>
        >>> model.training_summary
                        coefficients  degrees_freedom  standard_errors  \
        intercept_0        -0.780153                1              NaN
        Sepal_Length_1   -120.442165                1  28497036.888425
        Sepal_Length_0    -63.683819                1  28504715.870243
        intercept_1       -90.484405                1              NaN
        Petal_Length_0    117.979824                1  36178481.415888
        Petal_Length_1    206.339649                1  36172481.900910

                        wald_statistic   p_value
        intercept_0                NaN       NaN
        Sepal_Length_1       -0.000004  1.000000
        Sepal_Length_0       -0.000002  1.000000
        intercept_1                NaN       NaN
        Petal_Length_0        0.000003  0.998559
        Petal_Length_1        0.000006  0.998094

        >>> model.training_summary.covariance_matrix.inspect()
        [#]  Sepal_Length_0      Petal_Length_0      intercept_0
        ===============================================================
        [0]   8.12518826843e+14   -1050552809704907   5.66008788624e+14
        [1]  -1.05055305606e+15   1.30888251756e+15   -3.5175956714e+14
        [2]   5.66010683868e+14  -3.51761845892e+14  -2.52746479908e+15
        [3]   8.12299962335e+14  -1.05039425964e+15   5.66614798332e+14
        [4]  -1.05027789037e+15    1308665462990595    -352436215869081
        [5]     566011198950063  -3.51665950639e+14   -2527929411221601

        [#]  Sepal_Length_1      Petal_Length_1      intercept_1
        ===============================================================
        [0]     812299962806401  -1.05027764456e+15   5.66009303434e+14
        [1]  -1.05039450654e+15   1.30866546361e+15  -3.51663671537e+14
        [2]     566616693386615   -3.5243849435e+14   -2.5279294114e+15
        [3]    8.1208111142e+14   -1050119118230513   5.66615352448e+14
        [4]  -1.05011936458e+15   1.30844844687e+15   -3.5234036349e+14
        [5]     566617247774244  -3.52342642321e+14   -2528394057347494
        </skip>

        >>> predict_frame = model.predict(frame, ['Sepal_Length', 'Petal_Length'])
        <progress>

        >>> predict_frame.inspect()
        [#]  Sepal_Length  Petal_Length  Class  predicted_label
        =======================================================
        [0]           4.9           1.4      0                0
        [1]           4.7           1.3      0                0
        [2]           4.6           1.5      0                0
        [3]           6.3           4.9      1                1
        [4]           6.1           4.7      1                1
        [5]           6.4           4.3      1                1
        [6]           6.6           4.4      1                1
        [7]           7.2           6.0      2                2
        [8]           7.2           5.8      2                2
        [9]           7.4           6.1      2                2

        >>> test_metrics = model.test(frame, 'Class', ['Sepal_Length', 'Petal_Length'])
        <progress>

        >>> test_metrics
        accuracy         = 1.0
        confusion_matrix =             Predicted_0.0  Predicted_1.0  Predicted_2.0
        Actual_0.0              3              0              0
        Actual_1.0              0              4              0
        Actual_2.0              0              0              4
        f_measure        = 1.0
        precision        = 1.0
        recall           = 1.0

        >>> model.save("sandbox/logistic_regression")

        >>> restored = tc.load("sandbox/logistic_regression")

        >>> restored.training_summary.num_features == model.training_summary.num_features
        True

    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        """Loads a Logistic Regression Model from a scala model"""
        return LogisticRegressionModel(tc, scala_model)

    @property
    def observation_columns(self):
        """Column(s) containing the observations."""
        return self._tc.jutils.convert.from_scala_seq(self._scala.observationColumns())

    @property
    def label_column(self):
        """Column name containing the label for each observation."""
        return self._scala.labelColumn()

    @property
    def frequency_column(self):
        """Optional column containing the frequency of observations."""
        return self._scala.frequencyColumn()

    @property
    def num_classes(self):
        """Number of classes"""
        return self._scala.numClasses()

    @property
    def optimizer(self):
        """Set type of optimizer.
            LBFGS - Limited-memory BFGS.
            LBFGS supports multinomial logistic regression.
            SGD - Stochastic Gradient Descent.
            SGD only supports binary logistic regression."""
        return self._scala.optimizer()

    @property
    def compute_covariance(self):
        """Compute covariance matrix for the model."""
        return self._scala.computeCovariance()

    @property
    def intercept(self):
        """intercept column of training data."""
        return self._scala.intercept()

    @property
    def feature_scaling(self):
        """Perform feature scaling before training model."""
        return self._scala.featureScaling()

    @property
    def threshold(self):
        """Threshold for separating positive predictions from negative predictions."""
        return self._scala.threshold()

    @property
    def reg_type(self):
        """Set type of regularization
            L1 - L1 regularization with sum of absolute values of coefficients
            L2 - L2 regularization with sum of squares of coefficients"""
        return self._scala.regType()

    @property
    def reg_param(self):
        """Regularization parameter"""
        return self._scala.regParam()

    @property
    def num_iterations(self):
        """Maximum number of iterations"""
        return self._scala.numIterations()

    @property
    def convergence_tolerance(self):
        """Convergence tolerance of iterations for L-BFGS. Smaller value will lead to higher accuracy with the cost of more iterations."""
        return self._scala.convergenceTolerance()

    @property
    def num_corrections(self):
        """Number of corrections used in LBFGS update.
            Default is 10.
            Values of less than 3 are not recommended;
            large values will result in excessive computing time."""
        return self._scala.numCorrections()

    @property
    def mini_batch_fraction(self):
        """Fraction of data to be used for each SGD iteration"""
        return self._scala.miniBatchFraction()

    @property
    def step_size(self):
        """Initial step size for SGD. In subsequent steps, the step size decreases by stepSize/sqrt(t)"""
        return self._scala.stepSize()

    @property
    def training_summary(self):
        """Logistic regression summary table"""
        return LogisticRegressionSummaryTable(self._tc, self._scala.trainingSummary())

    def predict(self, frame, observation_columns_predict):
        """
        Predict labels for data points using trained logistic regression model.

        Predict the labels for a test frame using trained logistic regression model, and create a new frame revision with
        existing columns and a new predicted label's column.

        Parameters
        ----------

        :param frame: (Frame) A frame whose labels are to be predicted. By default, predict is run on the same columns
                              over which the model is trained.
        :param observation_columns_predict: (None or list[str]) Column(s) containing the observations whose labels are
                                            to be predicted. Default is the labels the model was trained on.
        :return: (Frame) Frame containing the original frame's columns and a column with the predicted label.
        """
        columns_option = self._tc.jutils.convert.to_scala_option_list_string(observation_columns_predict)
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.predict(frame._scala, columns_option))

    def test(self, frame, label_column, observation_columns_test):
        """
        Get the predictions for observations in a test frame

        Parameters
        ----------

        :param frame: (Frame) Frame whose labels are to be predicted.
        :param label_column: (str) Column containing the actual label for each observation.
        :param observation_columns_test: (None or list[str]) Column(s) containing the observations whose labels are to
                                         be predicted and tested. Default is to test over the columns the SVM model was
                                        trained on.
        :return: (ClassificationMetricsValue) Object with binary classification metrics
        """
        scala_classification_metrics_object = self._scala.test(frame._scala, label_column,
                                                               self._tc.jutils.convert.to_scala_option_list_string(
                                                               observation_columns_test))
        return ClassificationMetricsValue(self._tc, scala_classification_metrics_object)

    def save(self, path):
        """
        Save the trained model to path

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
        """

        if not isinstance(path, basestring):
            raise TypeError("path parameter must be a str, but received %s" % type(path))

        self._scala.exportToMar(path)
