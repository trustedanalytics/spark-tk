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
