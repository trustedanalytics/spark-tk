""" Generates data for gmm model
    params: n_samples: number of rows
            centers: number of centroids
            n_features: number of columns"""
from sklearn.datasets.samples_generator import make_blobs


def gen_data(n_rows, k, features):
    x,y = make_blobs(n_samples=n_rows, centers=k, n_features=features, random_state=14)
    for row in x.tolist():
        print ",".join(map(str,row))

gen_data(50, 5, 2)
