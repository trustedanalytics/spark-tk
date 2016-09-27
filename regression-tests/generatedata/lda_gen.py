import random
import math

def concat(list):
    return reduce(lambda l, c: l+c, list, [])

def makedata(topic_count, word_count, paper_count, common_words, number_topics_paper=1, word_count_min=1, word_count_max=20, common_word_count_min=10, common_word_count_max=100):

    # generate topic specific words
    # res :: [[[(string, string, string, string)]]]
    with open("lda_big.csv", "w", 10**9) as f:
        for paper in range(paper_count):
            for topicval in [random.randint(1, topic_count) for _ in range(number_topics_paper)]:
                for word in range(word_count):
                      f.write(','.join(("paper-"+str(paper),"word-"+str(word)+str(topicval), str(random.randint(word_count_min,word_count_max)), str(topicval), "\n")))

    # generate general words
    # res2 :: [[(string, string, string, string)]]
        for paper in range(paper_count):
            for word in range(common_words):
                f.write(','.join(("paper-"+str(paper),"word-"+str(word), str(int(math.ceil(random.uniform(common_word_count_min, common_word_count_max)))), "-1", "\n")))
    

if __name__ == '__main__':
    makedata(10000, 1000, 20000, 100000)
