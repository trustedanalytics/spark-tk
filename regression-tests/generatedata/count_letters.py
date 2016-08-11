
def generate():
    string = "abcdefghijklmnopqrstuvwxyz"
    for i in range(1000):
        print str(i)+","+string[i % len(string)]

if __name__ == "__main__":
    generate()
