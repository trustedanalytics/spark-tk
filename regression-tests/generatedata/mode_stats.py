from numpy import random

def generate_mode(num_rows):
    modes = [1, 3, 4, 5]
    with open("mode_stats.tsv", 'w') as f:
        for i in xrange(num_rows):
            if random.uniform(0,1) > 0.05:
                f.write(str(2) + "\t" + str(i) + "\n")
            else:
                f.write(str(random.choice(modes)) + "\t" + str(i) + "\n")
      
if __name__=="__main__":
    generate_mode(873)
