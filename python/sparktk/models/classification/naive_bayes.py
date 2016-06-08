from sparktk.loggers import log_load; log_load(__name__); del log_load

from sparktk.propobj import PropertiesObject


def train(frame, labelColumn, observationColumns, lambdaParameter = 1.0):
    """
    Creates a Naive Bayes by training on the given frame

    :param frame: frame of training data
    :param labelColumn Column containing the label for each observation
    :param observationColumns Column(s) containing the observations
    :param lambdaParameter Additive smoothing parameter Default is 1.0

    :return: NaiveBayesModel

    """
    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    scala_model = _scala_obj.train(frame._scala,
                                   labelColumn,
                                   tc.jutils.convert.to_scala_list_string(observationColumns),
                                   lambdaParameter)
    return NaiveBayesModel(tc, scala_model)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.classification.naive_bayes.NaiveBayesModel


class NaiveBayesModel(PropertiesObject):
    """
    A trained Naive Bayes model

    Example
    -------

    >>> frame = tc.frame.create([[1,19.8446136104,2.2985856384],[1,16.8973559126,2.6933495054],
    ...                                 [1,5.5548729596,2.7777687995],[0,46.1810010826,3.1611961917],
    ...                                 [0,44.3117586448,3.3458963222],[0,34.6334526911,3.6429838715]],
    ...                                 [('Class', int), ('Dim_1', float), ('Dim_2', float)])

    >>> model = tc.models.classification.naive_bayes.train(frame, 'Class', ['Dim_1', 'Dim_2'], 0.9)

    >>> model.label_column
    u'Class'

    >>> model.observation_columns
    [u'Dim_1', u'Dim_2']

    >>> model.lambda_parameter
    0.9

    >>> model.predict(frame, ['Dim_1', 'Dim_2'])

    >>> frame.inspect()
    [#]  Class  Dim_1          Dim_2         predicted_class
    ========================================================
    [0]      1  19.8446136104  2.2985856384              0.0
    [1]      1  16.8973559126  2.6933495054              1.0
    [2]      1   5.5548729596  2.7777687995              1.0
    [3]      0  46.1810010826  3.1611961917              0.0
    [4]      0  44.3117586448  3.3458963222              0.0
    [5]      0  34.6334526911  3.6429838715              0.0

    >>> model.save("sandbox/naivebayes")

    >>> restored = tc.load("sandbox/naivebayes")

    >>> restored.label_column == model.label_column
    True

    >>> restored.lambda_parameter == model.lambda_parameter
    True

    >>> set(restored.observation_columns) == set(model.observation_columns)
    True

    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def load(tc, scala_model):
        return NaiveBayesModel(tc, scala_model)

    @property
    def label_column(self):
        return self._scala.labelColumn()

    @property
    def observation_columns(self):
        return self._tc.jutils.convert.from_scala_seq(self._scala.observationColumns())

    @property
    def lambda_parameter(self):
        return self._scala.lambdaParameter()

    def predict(self, frame, columns=None):
        c = self.__columns_to_option(columns)
        self._scala.predict(frame._scala, c)

    def __columns_to_option(self, c):
        if c is not None:
            c = self._tc.jutils.convert.to_scala_list_string(c)
        return self._tc.jutils.convert.to_scala_option(c)

    def save(self, path):
        self._scala.save(self._tc._scala_sc, path)

del PropertiesObject
