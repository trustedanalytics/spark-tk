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

#!/usr/bin/python

import sys
import random

def get_row_type():
  num = random.randint(1,5)
  if num in range(1,4):
     return "tr"
  elif num is 4:
     return "va"
  elif num is 5:
     return "te"
  else:
     print "get_row_type() returned ", num
     sys.exit()



def main():
    if len(sys.argv) != 3:
      print "Usage: ./movie_user_3ratings.py number_of_movies number_of_user "
      sys.exit()
     
    number_of_movies = int(sys.argv[1])
    number_of_user = int(sys.argv[2])
    
    f = open("movie_user_3ratings.csv", 'w')
    for mov in range(1, number_of_movies):
      for user in range(1, number_of_user):
	row_type = get_row_type()
	weight = random.choice([1,2,5])
	
	#print '%d,%s,%d,%d,%s' % (-mov,'r',user,weight,row_type)
	#print '%d,%s,%d,%d,%s' % (user,'l',-mov,weight,row_type)
	
	f.write('%d,%s,%d,%d,%s' % (-mov,'r',user,weight,row_type) + "\n")
	f.write('%d,%s,%d,%d,%s' % (user,'l',-mov,weight,row_type) + "\n")
    f.close()

















if __name__ == "__main__":
    main()
