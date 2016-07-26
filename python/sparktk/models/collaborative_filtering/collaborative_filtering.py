from sparktk.loggers import log_load;

log_load(__name__);
del log_load

from sparktk.propobj import PropertiesObject

from collections import namedtuple

RecommendReturnTuple = namedtuple("RecommendReturnTuple", ['user', 'product', 'rating'])


def train(frame,
          source_column_name,
          dest_column_name,
          weight_column_name,
          max_steps=10,
          regularization=0.5,
          alpha=0.5,
          num_factors=3,
          use_implicit=False,
          num_user_blocks=2,
          num_item_block=3,
          checkpoint_iterations=10,
          target_rmse=0.05):
    """
    Create collaborative filtering model by training on given frame

    Parameters
    ----------

    :param frame: (Frame) The frame containing the data to train on
    :param source_column_name: (str) source column name.
    :param dest_column_name: (str) destination column name.
    :param weight_column_name: (str) weight column name.
    :param max_steps: (int) max number of super-steps (max iterations) before the algorithm terminates. Default = 10
    :param regularization: (float) value between 0 .. 1
    :param alpha: (double) value between 0 .. 1
    :param num_factors: (int) number of the desired factors (rank)
    :param use_implicit: (bool) use implicit preference
    :param num_user_blocks: (int) number of user blocks
    :param num_item_block: (int) number of item blocks
    :param checkpoint_iterations: (int) Number of iterations between checkpoints
    :param target_rmse: (double) target RMSE
    :return: (CollaborativeFilteringModel) A trained collaborative filtering model
    """

    tc = frame._tc
    _scala_obj = get_scala_obj(tc)
    scala_model = _scala_obj.train(frame._scala,
                                   source_column_name,
                                   dest_column_name,
                                   weight_column_name,
                                   max_steps,
                                   regularization,
                                   alpha,
                                   num_factors,
                                   use_implicit,
                                   num_user_blocks,
                                   num_item_block,
                                   checkpoint_iterations,
                                   target_rmse)
    return CollaborativeFilteringModel(tc, scala_model)


def get_scala_obj(tc):
    """Gets reference to the scala object"""
    return tc.sc._jvm.org.trustedanalytics.sparktk.models.collaborativefiltering.CollaborativeFilteringModel


def scala_collaborative_filtering_recommend_return_to_python(self, recommend_return):
    """
    method to convert scala CollaborativeFilteringRecommendReturn to python list
    :param recommend_return: scala recommend return result
    :return: return python list of tuples('user', 'product', 'rating')
    """
    scala_list_return = self._tc.sc._jvm.org.trustedanalytics.sparktk.models.collaborativefiltering.CollaborativeFilteringModel.scalaCollaborativeFilteringRecommendReturnToPython(
            recommend_return)
    python_list = [RecommendReturnTuple(user=recommend_list[0], product=recommend_list[1], rating=recommend_list[2])
                   for recommend_list in scala_list_return]
    return python_list


class CollaborativeFilteringModel(PropertiesObject):
    """
    A trained Collaborative Filtering Model

    Example
    -------

        >>> schema = [('source', int), ('dest', int), ('weight', float)]
        >>> rows = [ [1, 3, .5], [1, 4, .6], [1, 5, .7], [2, 5, .1] ]

        >>> frame = tc.frame.create(rows, schema)
        <progress>
        >>> frame.inspect()
        [#]  source  dest  weight
        =========================
        [0]       1     3     0.5
        [1]       1     4     0.6
        [2]       1     5     0.7
        [3]       2     5     0.1

        >>> rows_predict = [ [1, 3], [1, 4], [1, 5], [2, 5] ]
        >>> schema_predict = [('source', int), ('dest', int)]

        >>> predict_frame = tc.frame.create(rows_predict, schema_predict)
        <progress>

        >>> model = tc.models.collaborative_filtering.collaborative_filtering.train(frame, 'source', 'dest', 'weight')
        <progress>

        >>> predict_result = model.create_predict_frame(predict_frame, 'source', 'dest')
        <progress>
        <skip>
        >>> predict_result.inspect()
        [#]  user  product  rating
        ===============================
        [0]     1        4   0.04834617
        [1]     1        3  0.040288474
        [2]     2        5  0.003955772
        [3]     1        5  0.029929327
        </skip>
        <hide>
        >>> expected =[[1, 4, 0.04834617],[1, 3, 0.040288474],[2, 5, 0.003955772],[1, 5, 0.029929327]]

        >>> actual = predict_result.take(4).data

        >>> if len(expected) != len(actual):
        ...     raise RuntimeError("Mismatched lengths for predicted frame, expected %s != got %s" % (len(expected), len(actual)))

        >>> for i in xrange(len(actual)):
        ...     tc.testing.compare_floats(expected[2], actual[2], 0.001)
        </hide>
        >>> recommendations = model.recommend(1, 3, True)
        <progress>
        <skip>
        >>> recommendations
        [{u'rating': 0.04854799984010311, u'product': 4, u'user': 1}, {u'rating': 0.04045666535703035, u'product': 3, u'user': 1}, {u'rating': 0.030060528471388848, u'product': 5, u'user': 1}]
        </skip>
        >>> recommendations = model.recommend(5, 2, False)
        <progress>
        >>> recommendations = model.recommend(1, 3, True)
        <progress>
        <hide>
        >>> "%.2f" % recommendations[0]['rating']
        '0.05'
        >>> "%.2f" % recommendations[1]['rating']
        '0.04'
        >>> "%.2f" % recommendations[2]['rating']
        '0.03'
        >>> recommendations = model.recommend(3, 2, False)
        <progress>
        >>> "%.2f" % recommendations[0]['rating']
        '0.04'
        </hide>
    """

    def __init__(self, tc, scala_model):
        self._tc = tc
        tc.jutils.validate_is_jvm_instance_of(scala_model, get_scala_obj(tc))
        self._scala = scala_model

    @staticmethod
    def _from_scala(tc, scala_model):
        """Loads a collaborative filtering model from a scala model"""
        return CollaborativeFilteringModel(tc, scala_model)

    @property
    def source_column_name(self):
        """source column name used for model training"""
        return self._scala.sourceColumnName()

    @property
    def dest_column_name(self):
        """destination column name used for model training"""
        return self._scala.destColumnName()

    @property
    def weight_column_name(self):
        """weight column name used for model training"""
        return self._scala.weightColumnName()

    @property
    def max_steps(self):
        """maximum steps used for model training"""
        return self._scala.maxSteps()

    @property
    def regularization(self):
        """regularization used for model training"""
        return self._scala.regularization()

    @property
    def alpha(self):
        """alpha used for model training"""
        return self._scala.alpha()

    @property
    def num_factors(self):
        """number of desired factors(rank) used for model training"""
        return self._scala.numFactors()

    @property
    def use_implicit(self):
        """use implicit for model training"""
        return self._scala.useImplicit()

    @property
    def num_user_blocks(self):
        """number of user blocks used model training"""
        return self._scala.numUserBlocks()

    @property
    def num_item_block(self):
        """number of item blocks used for model training"""
        return self._scala.numItemBlock()

    @property
    def checkpoint_iterations(self):
        """check point iterations used for model training"""
        return self._scala.checkpointIterations()

    @property
    def target_rmse(self):
        """target RMSE used for model training"""
        return self._scala.targetRMSE()

    @property
    def user_frame(self):
        """user frame from model"""
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.userFrame())

    @property
    def product_frame(self):
        """user frame from model"""
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.productFrame())

    def create_predict_frame(self,
                             frame,
                             input_source_column_name,
                             input_dest_column_name,
                             output_user_column_name="user",
                             output_product_column_name="product",
                             output_rating_column_name="rating"):
        """
        Predicts the given frame based on trained model

        :param frame: (Frame) frame to predict based on generated model
        :param input_source_column_name: (str) source column name.
        :param input_dest_column_name: (str) destination column name.
        :param output_user_column_name: (str) A user column name for the output frame
        :param output_product_column_name: (str) A product  column name for the output frame
        :param output_rating_column_name: (str) A rating column name for the output frame
        :return: (Frame) returns predicted rating frame with specified output columns
        """
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.createPredictFrame(frame._scala,
                                                                    input_source_column_name,
                                                                    input_dest_column_name,
                                                                    output_user_column_name,
                                                                    output_product_column_name,
                                                                    output_rating_column_name))

    def recommend(self, entity_id, number_of_recommendations=1, recommend_products=True):
        """
        recommend products to users or vice versa

        :param entity_id: (int) A user/product id
        :param number_of_recommendations: (int) Number of recommendations
        :param recommend_products: (bool) True - products for user; false - users for the product
        :return: Returns an array of recommendations (as array of csv-strings)
        """
        # returns scala list of scala map
        scala_list_of_scala_map = self._scala.recommend(entity_id, number_of_recommendations, recommend_products)

        # First convert to python list of scala map
        python_list_of_scala_map = self._tc.jutils.convert.from_scala_seq(scala_list_of_scala_map)

        # Convert to Python list of python map
        python_list_of_python_map = []
        for scala_map in python_list_of_scala_map:
            python_list_of_python_map.append(self._tc.jutils.convert.scala_map_to_python(scala_map))

        return python_list_of_python_map

    def save(self, path):
        """
        save the trained model to path

        :param path: (str) Path to save
        """
        self._scala.save(self._tc._scala_sc, path)
