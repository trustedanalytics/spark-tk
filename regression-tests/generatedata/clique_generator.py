
def k_gen(src, n, prefix):
    return [(prefix+str(src), prefix+str(dst+1)) for dst in range(n)]

def generate_cliques(clique_count):

    for i in range(2, clique_count):
        edges = [k_gen(j, j-1, "k_"+str(i)+"_") for j in range(2, i+1)]
        flattened_edges = [inner for outer in edges for inner  in outer]
        for (i, j) in flattened_edges:
            print str(i)+","+str(j)


        

if __name__ == "__main__":
    generate_cliques(11)
