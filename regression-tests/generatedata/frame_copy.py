from random import randint
"""Generate data for frame_copy test"""

def generate_data(lines):
    for i in range(lines):
        print("string," + str(i%10))

if __name__ == "__main__":
    generate_data(100)

