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
      print "Usage: ./netflix_1_2_5.py number_of_movies number_of_user "
      sys.exit()
     
    number_of_movies = int(sys.argv[1])
    number_of_user = int(sys.argv[2])
    
    f = open("netf_1_2_5.csv", 'w')
    for mov in range(1, number_of_movies):
      for user in range(1, number_of_user):
	row_type = get_row_type()
	weight = random.choice([1, 2, 5])
	
	#print '%d,%s,%d,%d,%s' % (-mov,'r',user,weight,row_type)
	#print '%d,%s,%d,%d,%s' % (user,'l',-mov,weight,row_type)
	
	f.write('%d,%s,%d,%d,%s' % (-mov,'r',user,weight,row_type) + "\n")
	f.write('%d,%s,%d,%d,%s' % (user,'l',-mov,weight,row_type) + "\n")
    f.close()

















if __name__ == "__main__":
    main()
