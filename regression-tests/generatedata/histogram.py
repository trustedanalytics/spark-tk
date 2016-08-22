from numpy import random 

def generate_histogram(num_rows):
    with open("histogram.csv", 'w') as f:
        for i in xrange(0, num_rows):
            f.write(str(int(random.uniform(1, 10))) + "\n")

if __name__ == "__main__":
    generate_histogram(100)
