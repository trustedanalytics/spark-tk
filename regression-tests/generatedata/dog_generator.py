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

owner_names = ["Asha", "Haley", "Lewis", "Georgia"]
dog_names = ["Fluffy", "Rex", "Spot", "Sparky", "Button", "Fido", "Rover", "Skipper", "Fifi", "Scooby", "Bandit", "Buddy"]
hair_types = ["long", "short"]
max_weight = 100
min_weight = 5
max_age = 20
min_age = 0


def generate_dogs(number):
    dogs = []
    csv = open("dogs.csv", "w")
    csv = open("../datasets/dogs.csv", "w")
    for index in range(0, number):
        weight = random.randint(min_weight, max_weight)
        age = random.randint(min_age, max_age)
        hair_type = hair_types[random.randint(0, len(hair_types) - 1)]
        dog_name = dog_names[random.randint(0, len(dog_names) - 1)]
        owner_name = owner_names[random.randint(0, len(owner_names) - 1)]
        
        new_dog = str(age) + "," +  dog_name + "," + owner_name + "," + str(weight) + "," +  hair_type + "\n"
        csv.write(new_dog)
