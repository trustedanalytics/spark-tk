# vim: set encoding=utf-8 
"""binary classification metrics dataset generator"""

def generate_data(rows, positive, correct):
    tp = rows * positive * correct
    fp = rows * (1-positive) * (1-correct)
    tn = rows * (1-positive) * correct
    fn = rows * positive * (1-correct)

    for i in range(int(tp)):
        print "1,1"
    for i in range(int(fp)):
        print "0,1"
    for i in range(int(tn)):
        print  "0,0"
    for i in range(int(fn)):
        print "1,0"

if __name__ == "__main__":
    generate_data(2*10**2, 0.4, .8)
