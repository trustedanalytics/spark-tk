from sparktk.loggers import log_load; log_load(__name__); del log_load

from sparktk.propobj import PropertiesObject
from sparktk.lazyloader import implicit

def train(frame,
          document_column_name,
          word_column_name,
          word_count_column_name,
          max_iterations = 20,
          alpha = None,
          beta = 1.1,
          num_topics = 10,
          random_seed = None):
    """
    Creates a LdaModel by training on the given frame
    See the discussion about `Latent Dirichlet Allocation at Wikipedia. <http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation>`
    :param frame: (Frame) Input frame data
    :param documentColumnName: (str) Column Name for documents. Column should contain a str value.
    :param wordColumnName: (str) Column name for words. Column should contain a str value.
    :param wordCountColumnName: (str) Column name for word count. Column should contain an int32 or int64 value.
    :param maxIterations: (int) The maximum number of iterations that the algorithm will execute.
                         The valid value range is all positive int. Default is 20.
    :param alpha: (Optional(List(float))) The :term:`hyperparameter` for document-specific distribution over topics.
                  Mainly used as a smoothing parameter in :term:`Bayesian inference`.
                  If set to a singleton list List(-1d), then docConcentration is set automatically.
                  If set to singleton list List(t) where t != -1, then t is replicated to a vector of length k during
                  LDAOptimizer.initialize(). Otherwise, the alpha must be length k.
                  Currently the EM optimizer only supports symmetric distributions, so all values in the vector should be the same.
                  Values should be greater than 1.0. Default value is -1.0 indicating automatic setting.
    :param beta: (float) The :term:`hyperparameter` for word-specific distribution over topics.
                 Mainly used as a smoothing parameter in :term:`Bayesian inference`.
                 smaller value implies that topics are more concentrated on a small
                 subset of words.
                 Valid value range is all positive float greater than or equal to 1.
                 Default is 0.1.
    :param numTopics: (int) The number of topics to identify in the LDA model.
                      Using fewer topics will speed up the computation, but the extracted topics
                      might be more abstract or less specific; using more topics will
                      result in more computation but lead to more specific topics.
                      Valid value range is all positive int.
                      Default is 10.
    :param randomSeed: (Optional(long)) An optional random seed.
                      The random seed is used to initialize the pseudorandom number generator
                      used in the LDA model. Setting the random seed to the same value every
                      time the model is trained, allows LDA to generate the same topic distribution
                      if the corpus and LDA parameters are unchanged.
    :return: (LdaModel) Trained Lda Model


    """
    tc = frame._tc
    _scala_obj = get_scala_obj(tc)

    scala_alpha = tc.jutils.convert.to_scala_option_list_double(alpha)
    random_seed = random_seed if random_seed is None else long(random_seed)
    scala_seed = tc.jutils.convert.to_scala_option(random_seed)
    scala_model = _scala_obj.train(frame._scala,
                                   document_column_name,
                                   word_column_name,
                                   word_count_column_name,
                                   max_iterations,
                                   scala_alpha,
                                   beta,
                                   num_topics,
                                   scala_seed)
    return LdaModel(tc, scala_model)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.clustering.lda.LdaModel


def load(path, tc=implicit):
    """load LdaModel from given path"""
    if tc is implicit:
        implicit.error("tc")
    return tc.load(path, LdaModel)


class LdaModel(PropertiesObject):
    """
    A trained Lda model

    Example
    -------

        >>> frame = tc.frame.create([['nytimes','harry',3], ['nytimes','economy',35], ['nytimes','jobs',40], ['nytimes','magic',1],
        ...                                 ['nytimes','realestate',15], ['nytimes','movies',6],['economist','economy',50],
        ...                                 ['economist','jobs',35], ['economist','realestate',20],['economist','movies',1],
        ...                                 ['economist','harry',1],['economist','magic',1],['harrypotter','harry',40],
        ...                                 ['harrypotter','magic',30],['harrypotter','chamber',20],['harrypotter','secrets',30]],
        ...                                 [('doc_id', str), ('word_id', str), ('word_count', long)])

        >>> frame.inspect()
        [#]  doc_id     word_id     word_count
        ======================================
        [0]  nytimes    harry                3
        [1]  nytimes    economy             35
        [2]  nytimes    jobs                40
        [3]  nytimes    magic                1
        [4]  nytimes    realestate          15
        [5]  nytimes    movies               6
        [6]  economist  economy             50
        [7]  economist  jobs                35
        [8]  economist  realestate          20
        [9]  economist  movies               1

        >>> model = tc.models.clustering.lda.train(frame, 'doc_id', 'word_id', 'word_count', max_iterations = 3, num_topics = 2)

        >>> print model.report
        ======Graph Statistics======
        Number of vertices: 11} (doc: 3, word: 8})
        Number of edges: 16
        <blankline>
        ======LDA Configuration======
        numTopics: 2
        alpha: 26.0
        beta: 1.100000023841858
        maxIterations: 3
        <blankline>

        >>> model.document_column_name
        u'doc_id'

        >>> model.word_column_name
        u'word_id'

        >>> model.max_iterations
        3

        >>> model.training_data_row_count
        16L

        >>> model.topics_given_doc_frame.schema
        [(u'doc_id', <type 'unicode'>), (u'topic_probabilities', vector(2))]

        <skip>
        >>> model.topics_given_doc_frame.inspect(columns = ['doc_id'])
        [#]  doc_id       topic_probabilities
        ===========================================================
        [0]  harrypotter  [0.06417509902256538, 0.9358249009774346]
        [1]  economist    [0.8065841283073141, 0.19341587169268581]
        [2]  nytimes      [0.855316939742769, 0.14468306025723088]

        >>> model.word_given_topics_frame.inspect()
        [#]  word_id     topic_probabilities
        =============================================================
        [0]  harry       [0.005015572372943657, 0.2916109787103347]
        [1]  realestate  [0.167941871746252, 0.032187084858186256]
        [2]  secrets     [0.026543839878055035, 0.17103864163730945]
        [3]  movies      [0.03704750433384287, 0.003294403360133419]
        [4]  magic       [0.016497495727347045, 0.19676900962555072]
        [5]  economy     [0.3805836266747442, 0.10952481503975171]
        [6]  chamber     [0.0035944004256137523, 0.13168123398523954]
        [7]  jobs        [0.36277568884120137, 0.06389383278349432]

        >>> model.topics_given_word_frame.inspect()
        [#]  word_id     topic_probabilities
        ===========================================================
        [0]  harry       [0.018375903962878668, 0.9816240960371213]
        [1]  realestate  [0.8663322126823493, 0.13366778731765067]
        [2]  secrets     [0.15694172611285945, 0.8430582738871405]
        [3]  movies      [0.9444179131148587, 0.055582086885141324]
        [4]  magic       [0.09026309091077593, 0.9097369090892241]
        [5]  economy     [0.8098866029287505, 0.19011339707124958]
        [6]  chamber     [0.0275551649439219, 0.9724448350560781]
        [7]  jobs        [0.8748608515169193, 0.12513914848308066]
        </skip>


        >>> prediction = model.predict(['harry', 'secrets', 'magic', 'harry', 'chamber' 'test'])

        <skip>
        >>> prediction
        {u'topics_given_doc': [0.3149285399451628, 0.48507146005483726], u'new_words_percentage': 20.0, u'new_words_count': 1}
        >>> prediction['topics_given_doc']
        [0.3149285399451628, 0.48507146005483726]
        </skip>
        >>> prediction['new_words_percentage']
        20.0
        >>> prediction['new_words_count']
        1
        >>> prediction.has_key('topics_given_doc')
        True
        >>> prediction.has_key('new_words_percentage')
        True
        >>> prediction.has_key('new_words_count')
        True

        >>> model.save("sandbox/lda")

        >>> restored = tc.load("sandbox/lda")

        >>> restored.document_column_name == model.document_column_name
        True

        >>> restored.max_iterations == model.max_iterations
        True

        >>> restored.topics_given_doc_frame.schema
        [(u'doc_id', <type 'unicode'>), (u'topic_probabilities', vector(2))]

        <skip>
        >>> restored.topics_given_doc_frame.inspect()
        [#]  doc_id       topic_probabilities
        ===========================================================
        [0]  harrypotter  [0.06417509902256538, 0.9358249009774346]
        [1]  economist    [0.8065841283073141, 0.19341587169268581]
        [2]  nytimes      [0.855316939742769, 0.14468306025723088]
        </skip>


    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        return LdaModel(tc, scala_model)

    @property
    def document_column_name(self):
        """Column Name for documents"""
        return self._scala.documentColumnName()

    @property
    def word_column_name(self):
        """Column name for words"""
        return self._scala.wordColumnName()

    @property
    def word_count_column_name(self):
        """Column name for word count"""
        return self._scala.wordCountColumnName()

    @property
    def max_iterations(self):
        """The maximum number of iterations that the algorithm could have executed"""
        return self._scala.maxIterations()

    @property
    def alpha(self):
        """Hyperparameter for document-specific distribution over topics"""
        s = self._tc.jutils.convert.from_scala_option(self._scala.alpha())
        if s:
            return list(self._tc.jutils.convert.from_scala_seq(s))
        return None

    @property
    def beta(self):
        """Hyperparameter for word-specific distribution over topics"""
        return self._scala.beta()

    @property
    def num_topics(self):
        """Number of topics to identify in the LdaModel"""
        return self._scala.numTopics()

    @property
    def random_seed(self):
        """Random seed used to train the model"""
        return self._tc.jutils.convert.from_scala_option(self._scala.randomSeed())

    @property
    def report(self):
        """Summary Report of the Training"""
        return self._scala.report()

    @property
    def topics_given_doc_frame(self):
        """Frame for topics given document"""
        return self._tc.frame.create(self._scala.topicsGivenDocFrame())

    @property
    def word_given_topics_frame(self):
        """Frame for word given topics"""
        return self._tc.frame.create(self._scala.wordGivenTopicsFrame())

    @property
    def topics_given_word_frame(self):
        """Frame for topics given word"""
        return self._tc.frame.create(self._scala.topicsGivenWordFrame())

    @property
    def training_data_row_count(self):
        """Row count of the frame used to train this model"""
        return self._scala.trainingDataRowCount()

    def predict(self, documents):
        """Predict topic probabilities for the documents given the trained model"""
        scala_documents = self._tc.jutils.convert.to_scala_list(documents)
        predicted_output = self._scala.predict(scala_documents)

        topics_given_doc = self._tc.jutils.convert.from_scala_seq(predicted_output.topicsGivenDoc())
        new_words_count = predicted_output.newWordsCount()
        new_words_percentage = predicted_output.newWordsPercentage()

        return {u"topics_given_doc":topics_given_doc,
                u"new_words_count":new_words_count,
                u"new_words_percentage":new_words_percentage}

    def save(self, path):
        """Save the trained model"""
        self._scala.save(self._tc._scala_sc, path)

del PropertiesObject
