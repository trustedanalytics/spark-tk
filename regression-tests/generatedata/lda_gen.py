# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

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
