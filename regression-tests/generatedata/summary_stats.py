from itertools import cycle

def generate_mode(num_rows):
    choice = cycle([10, 20, 30, 40, 50])
    with open("../datasets/SummaryStats2.csv", 'w') as f:
        for i in xrange(0, num_rows):
            f.write(str(next(choice)) + "," + str(600) + "\n")
        f.write("250" + "," + "3.1\n" +
                "-300" + "," + "1\n" + 
                "100" + "," + "-5000\n" +
                "123" + "," + "2\n" +
                "2000" + "," + "3\n" +
                "2000" + "," + "3")

if __name__ == "__main__":
    generate_mode(50)
