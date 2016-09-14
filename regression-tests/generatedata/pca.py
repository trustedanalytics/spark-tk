import random
import math
random.seed(10)

def data_generate(n):
    for i in range(n):
       print ','.join(map(lambda x: str(random.randint(1,20)), range(10)))

if __name__ == "__main__":
    data_generate(10000)
