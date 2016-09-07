import random


def generate_vector(num_items):
    csv = open("../datasets/vector.csv", 'w')
    for index in range(0, num_items):
        number = random.randint(0,100)
        line = str(number) + "\n"
        csv.write(line)


if __name__ == "__main__":
    generate_vector(100)
