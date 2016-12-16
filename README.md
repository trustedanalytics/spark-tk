[![Build Status](https://travis-ci.org/trustedanalytics/spark-tk.svg?branch=master)](https://travis-ci.org/trustedanalytics/spark-tk)


# spark-tk

**spark-tk** is a library which enhances the Spark experience by providing [a rich, easy-to-use API for Python and
Scala](http://trustedanalytics.github.io/spark-tk/).  It adds new machine learning capabilities and other operations,
like working with DICOM images for example.


## Overview
Spark-tk simplifies applying machine learning to big data for superior knowledge, discovery and predictive modeling
across a wide variety of use cases and solutions. Its APIs span feature engineering, graph construction, and various
types of machine learning. The APIs are geared at an abstraction level familiar to data scientists (similar to Python
pandas, scikit-learn) and removes the complexity of cluster computing and parallel processing.  The library works
alongside Spark and makes it easier to program.  The lower-level Spark APIs are also seamlessly exposed through the
library.  Applications written with Spark-tk will have access the best of both worlds for the given situation.   All
functionality operates at full scale according to the Spark configuration.  

### Frame Interface
Spark-tk uses a Frame object for its scalable data frame representation, which is familiar and intuitive to data
researchers compared to low level HDFS file and Spark RDD/DataFrame/DataSet formats. The library provides an API to
manipulate the data frames for feature engineering and exploration, such as joins and aggregations. User-defined
transformations and filters can be written and applied to large data sets using distributed processing. 

### Graph Analytics
Spark-tk uses a Graph object for its scalable graph representation, based on a Frame holding vertices and another Frame
holding edges.  Graph representations are broadly useful.

 + **Use Case:** linking disparate data with arbitrary edge types and then analyzing the connections for powerful
  predictive signals that can otherwise be missed with entity-based methods.
 
Working with graph representations can often be more intuitive and computationally efficient for data sets where the
connections between data observations are more numerous and more important than the data points alone.  Spark-tk brings
together the capabilities to create and analyze graphs, including engineering features and applying graph-based
algorithms. Since the graphs are built using frames, Frame operations may be seamlessly applied to graphs.

 + **Use Case:** applying a clustering algorithm to a vertex list with features developed using graph analytics.

Spark-tk supports importing and exporting graphs to the OrientDB's scalable graph database. Graph databases 
allow users to run real-time queries on their graph data.


### Machine Learning
The toolkit provides algorithms for supervised, unsupervised, and semi-supervised machine learning using both entity
and graphical machine learning tools.  Examples include time-series analysis, recommender systems using collaborative 
filtering, topic modeling using Latent Dirichlet Allocation, clustering using K-means, and classification using logistic regression. 
Available graph algorithms such as label propagation and loopy belief propagation exploit the connections in the graph 
structure and provide powerful new methods of labeling or classifying graph data.  Most of the Machine Learning is exposed 
through the Models API.  The Models API provides a simplified interface for data scientists to create, train, and test the performance
of their models. The trained models can then be used for predictions, classifications and recommendations. Data scientists can also
persist models by using the model save and load methods.


### Image Processing
Spark-tk includes support for ingesting and processing DICOM images in a distributed environment.  DICOM is the
international standard for medical images and related information (ISO 12052).  Sparktk provides queries, filters, and
analytics on collections of these images.


### Documentation

API Reference pages for Python and Scala are located [here](http://trustedanalytics.github.io/spark-tk/).


## Example:

Create a TkContext

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
    [[5.6000000000000005], [15.333333333333332], [20.0]]

Add cluster predictions to the frame

    >>> pf = km.predict(frame1)

    >>> pf.inspect()
    [#]  a  b  c   cluster
    ======================
    [0]  2  3   7        0
    [1]  1  4   6        0
    [2]  7  1  15        1
    [3]  1  1   3        0
    [4]  9  2  20        2
    [5]  2  4   8        0
    [6]  0  4   4        0
    [7]  6  3  15        1
    [8]  5  6  16        1

Upload some new data and predict

    >>> frame2 = tc.frame.create([[3], [8], [16], [1], [13], [18]])

    >>> pf2 = km.predict(frame2, 'C0')

    >>> pf2.inspect()
    [#]  C0  cluster
    ================
    [0]   3        0
    [1]   8        0
    [2]  16        1
    [3]   1        0
    [4]  13        1
    [5]  18        2

