from sparktk.loggers import log_load
from sparktk.propobj import PropertiesObject

log_load(__name__)
del log_load


def train(frame,
          label_column,
          observation_columns,
          frequency_column = None,
          num_classes = 2,
          optimizer = "LBFGS",
          compute_covariance = True,
          intercept = True,
          feature_scaling = False,
          threshold = 0.5,
          reg_type = "L2",
          reg_param = 0,
          num_iterations = 100,
          convergence_tolerance = 0.0001,
          num_corrections = 10,
          mini_batch_fraction = 1.0,
          step_size = 1.0):

    """
     Build logistic regression model.

     Creating a Logistic Regression Model using the observation column and label column of the train frame.

    Parameters
    ----------

    :param frame: (Frame) A frame to train the model on.
    :param label_column: (str) Column name containing the label for each observation.
    :param observation_columns: (List[str]) Column(s) containing the observations.
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
    :param mini_batch_fraction: () Fraction of data to be used for each SGD iteration
    :param stepSize Initial step size for SGD. In subsequent steps, the step size decreases by stepSize/sqrt(t)
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

def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.classification.logistic_regression






