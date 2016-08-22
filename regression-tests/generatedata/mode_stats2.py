from itertools import cycle

def generate_mode(num_rows):
    choice = cycle([10, 20, 30, 40, 50])
    with open("../datasets/mode_stats2.csv", 'w') as f:
        for i in xrange(0, num_rows):
            f.write(str(next(choice)) + "," + str(4) + "\n")

if __name__ == "__main__":
    generate_mode(50)
