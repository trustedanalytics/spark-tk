from numpy import random


def generate_model_color(num_rows):
    choices = ['red', 'green', 'blue', 'yellow']
    with open("../datasets/model_color.csv", 'w') as f:
        for i in xrange(0, num_rows):
            f.write(random.choice(choices) + "," +
                    random.choice(choices) + "\n")

    f.close()


if __name__ == "__main__":
    generate_model_color(20000)
  
        
