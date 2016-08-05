[![Build Status](https://travis-ci.org/trustedanalytics/spark-tk.svg?branch=master)](https://travis-ci.org/trustedanalytics/spark-tk)


# spark-tk

**spark-tk** is a library which enhances the Spark experience by providing a rich, easy-to-use API for Python and
Scala.

Tabular data is abstracted as **Frames** where expected operations are available and easy to call using column names.
You don’t need to know the details of Spark’s many APIs.  However, if you want to leverage them, it is easy to dip 
into those APIs and the functional programming model provided by Spark.

The library provides **machine learning** support through straightforward APIs to train and use various models.

## Example:

[//]:# "<skip>"

    >>> from sparktk import TkContext
    
    >>> tc = TkContext()
    
[//]:# "</skip>"

Upload some tabular data
    
    >>> frame1 = tc.frame.create(data=[[2, 3],
    ...                                [1, 4],
    ...                                [7, 1],
    ...                                [1, 1],
    ...                                [9, 2],
    ...                                [2, 4],
    ...                                [0, 4],
    ...                                [6, 3],
    ...                                [5, 6]],
    ...                          schema=[("a", int), ("b", int)])
    
    
Do a linear transform
    
    >>> frame1.add_columns(lambda row: row.a * 2 + row.b, schema=("c", int))
    
    >>> frame1.inspect()
    [#]  a  b  c
    =============
    [0]  2  3   7
    [1]  1  4   6
    [2]  7  1  15
    [3]  1  1   3
    [4]  9  2  20
    [5]  2  4   8
    [6]  0  4   4
    [7]  6  3  15
    [8]  5  6  16

Train a K-Means model

    >>> km = tc.models.clustering.kmeans.train(frame1, "c", k=3, seed=5)
  
    >>> km.centroids
    [[7.0], [16.5], [3.5]]

Add cluster predictions to the frame

    >>> km.predict(frame1)

    >>> frame1.inspect()
    [#]  a  b  c   cluster
    ======================
    [0]  2  3   7        0
    [1]  1  4   6        0
    [2]  7  1  15        1
    [3]  1  1   3        2
    [4]  9  2  20        1
    [5]  2  4   8        0
    [6]  0  4   4        2
    [7]  6  3  15        1
    [8]  5  6  16        1


Upload some new data and predict

    >>> frame2 = tc.frame.create([[3], [8], [16], [1], [13], [18]])

    >>> km.predict(frame2, 'C0')

    >>> frame2.inspect()
    [#]  C0  cluster
    ================
    [0]   3        2
    [1]   8        0
    [2]  16        1
    [3]   1        2
    [4]  13        1
    [5]  18        1

