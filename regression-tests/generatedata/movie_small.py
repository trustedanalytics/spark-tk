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

"""generates a csv file with data about fictional dogs"""
import random

max_weight = 100
min_weight = 5
max_age = 20
min_age = 0
min_rating = 1
max_rating = 5
min_views = 0
max_views = 1000
min_version = 0
max_version = 3


def generate_movies(number):
    csv = open("../datasets/movie_small.csv", "w")
    for index in range(0, number):
        weight = random.randint(min_weight, max_weight)
        age = random.randint(min_age, max_age)
        rating = random.randint(min_rating, max_rating)
        views = random.randint(min_views, max_views)
        version = random.randint(min_version, max_version)
        
        new_movie = str(age) + "," + str(rating)  + "," + str(views) + "," + str(weight) + "," +  str(version) + "\n"
        csv.write(new_movie)
