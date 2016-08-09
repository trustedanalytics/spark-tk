from setup import tc, rm, get_sandbox_path

import logging
logger = logging.getLogger(__name__)

def test_lda(tc):

    logger.info("define dataset for LDA")
    data = [["nytimes","harry",3],
             ["nytimes","economy",35],
             ["nytimes","jobs",40],
             ["nytimes","magic",1],
             ["nytimes","realestate",15],
             ["nytimes","movies",6],
             ["economist","economy",50],
             ["economist","jobs",35],
             ["economist","realestate",20],
             ["economist","movies",1],
             ["economist","harry",1],
             ["economist","magic",1],
             ["harrypotter","harry",40],
             ["harrypotter","magic",30],
             ["harrypotter","chamber",20],
             ["harrypotter","secrets",300]]

    logger.info("create frame")
    frame = tc.frame.create(data, schema= [('doc_id', str),
                                          ('word_id', str),
                                          ('word_count', long)])


    logger.info("inspect frame")
    frame.inspect(20)
    logger.info("frame row count " + str(frame.count()))

    model = tc.models.clustering.lda.train(frame,
                          'doc_id', 'word_id', 'word_count',
                          max_iterations = 3,
                          num_topics = 2)

    topics_given_doc = model.topics_given_doc_frame
    word_given_topics = model.word_given_topics_frame
    topics_given_word = model.topics_given_word_frame
    report = model.report


    assert report == "======Graph Statistics======\nNumber of vertices: 11} (doc: 3, word: 8})\n" \
                     "Number of edges: 16\n\n======LDA Configuration======\nnumTopics: 2\nalpha: 26.0\n" \
                     "beta: 1.100000023841858\nmaxIterations: 3\n"

    topics_given_doc.inspect()
    word_given_topics.inspect()
    topics_given_word.inspect()
    logger.info(report)

    logger.info("compute topic probabilities for document")
    prediction = model.predict(['harry', 'economy', 'magic', 'harry' 'test'])
    logger.info(prediction)

    logger.info("compute lda score")
    topics_given_doc.rename_columns({'topic_probabilities' : 'lda_topic_given_doc'})
    word_given_topics.rename_columns({'topic_probabilities' : 'lda_word_given_topic'})

    frame= frame.join_left(topics_given_doc, left_on="doc_id", right_on="doc_id")
    frame= frame.join_left(word_given_topics, left_on="word_id", right_on="word_id")

    frame.dot_product(['lda_topic_given_doc'], ['lda_word_given_topic'], 'lda_score')

    logger.info("compute histogram of scores")

    word_hist = frame.histogram('word_count')
    lda_hist = frame.histogram('lda_score')

    group_frame = frame.group_by('word_id_L', {'word_count': tc.agg.histogram(word_hist.cutoffs), 'lda_score':  tc.agg.histogram(lda_hist.cutoffs)})
    group_frame.inspect()
    assert group_frame.count() is 8
