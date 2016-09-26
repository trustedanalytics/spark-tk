import logging
logger = logging.getLogger('sparktk')
from graphframes.graphframe import GraphFrame, _from_java_gf
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import IllegalArgumentException
from sparktk.frame.frame import Frame
from sparktk.arguments import require_type


# import constructors for the API's sake (not actually dependencies of the Graph)
from sparktk.graph.constructors.create import create
from sparktk.graph.constructors.import_orientdb_graph import import_orientdb_graph

__all__ = ["create",
           "Graph",
           "import_orientdb_graph"]

class Graph(object):
    """
    sparktk Graph

    Represents a graph with a frame defining vertices and another frame defining edges.  It is implemented as a very
    thin wrapper of the spark GraphFrame (https://github.com/graphframes/graphframes) onto which additional methods
    are available.

    A vertices frame defines the vertices for the graph and must have a schema with a column
    named "id" which provides unique vertex ID.  All other columns are treated as vertex properties.
    If a column is also found named "vertex_type", it will be used as a special label to denote the type
    of vertex, for example, when interfacing with logic (such as a graph DB) which expects a
    specific vertex type.

    An edge frame defines the edges of the graph; schema must have columns names "src" and "dst" which provide the
    vertex ids of the edge.  All other columns are treated as edge properties.  If a column is also found named
    "edge_type", it will be used as a special label to denote the type of edge, for example, when interfacing with logic
    (such as a graph DB) which expects a specific edge type.

    Unlike sparktk Frames, this Graph is immutable (it cannot be changed).  The vertices and edges may be extracted as
    sparktk Frame objects, but those frames then take on a life of their own apart from this graph object.  Those frames
    could be transformed and used to build a new graph object if necessary.

    The underlying spark GraphFrame is available as the 'graphframe' property of this object.


    Examples
    --------

        >>> viewers = tc.frame.create([['fred', 0],
        ...                            ['wilma', 0],
        ...                            ['pebbles', 1],
        ...                            ['betty', 0],
        ...                            ['barney', 0],
        ...                            ['bamm bamm', 1]],
        ...                           schema= [('id', str), ('kids', int)])

        >>> titles = ['Croods', 'Jurassic Park', '2001', 'Ice Age', 'Land Before Time']

        >>> movies = tc.frame.create([[t] for t in titles], schema=[('id', str)])

        >>> vertices = viewers.copy()

        >>> vertices.append(movies)

        >>> vertices.inspect(20)
        [##]  id                kids
        ============================
        [0]   fred                 0
        [1]   wilma                0
        [2]   pebbles              1
        [3]   betty                0
        [4]   barney               0
        [5]   bamm bamm            1
        [6]   Croods            None
        [7]   Jurassic Park     None
        [8]   2001              None
        [9]   Ice Age           None
        [10]  Land Before Time  None

        >>> edges = tc.frame.create([['fred','Croods',5],
        ...                          ['fred','Jurassic Park',5],
        ...                          ['fred','2001',2],
        ...                          ['fred','Ice Age',4],
        ...                          ['wilma','Jurassic Park',3],
        ...                          ['wilma','2001',5],
        ...                          ['wilma','Ice Age',4],
        ...                          ['pebbles','Croods',4],
        ...                          ['pebbles','Land Before Time',3],
        ...                          ['pebbles','Ice Age',5],
        ...                          ['betty','Croods',5],
        ...                          ['betty','Jurassic Park',3],
        ...                          ['betty','Land Before Time',4],
        ...                          ['betty','Ice Age',3],
        ...                          ['barney','Croods',5],
        ...                          ['barney','Jurassic Park',5],
        ...                          ['barney','Land Before Time',3],
        ...                          ['barney','Ice Age',5],
        ...                          ['bamm bamm','Croods',5],
        ...                          ['bamm bamm','Land Before Time',3]],
        ...                         schema = ['src', 'dst', 'rating'])

        >>> edges.inspect(20)
        [##]  src        dst               rating
        =========================================
        [0]   fred       Croods                 5
        [1]   fred       Jurassic Park          5
        [2]   fred       2001                   2
        [3]   fred       Ice Age                4
        [4]   wilma      Jurassic Park          3
        [5]   wilma      2001                   5
        [6]   wilma      Ice Age                4
        [7]   pebbles    Croods                 4
        [8]   pebbles    Land Before Time       3
        [9]   pebbles    Ice Age                5
        [10]  betty      Croods                 5
        [11]  betty      Jurassic Park          3
        [12]  betty      Land Before Time       4
        [13]  betty      Ice Age                3
        [14]  barney     Croods                 5
        [15]  barney     Jurassic Park          5
        [16]  barney     Land Before Time       3
        [17]  barney     Ice Age                5
        [18]  bamm bamm  Croods                 5
        [19]  bamm bamm  Land Before Time       3

        >>> graph = tc.graph.create(vertices, edges)

        >>> graph
        Graph(v:[id: string, kids: int], e:[src: string, dst: string, rating: int])

        <hide>
        # compare this graph construction with the examples' "movie" graph to insure they stay in sync!

        >>> example = tc.examples.graphs.get_movie_graph()

        >>> assert str(graph) == str(example)

        >>> assert str(example.create_vertices_frame().inspect(20)) == str(vertices.inspect(20))

        >>> assert str(example.create_edges_frame().inspect(20)) == str(edges.inspect(20))

        </hide>

        >>> graph.save("sandbox/old_movie_graph")

        >>> restored = tc.load("sandbox/old_movie_graph")

        >>> restored
        Graph(v:[id: string, kids: int], e:[src: string, dst: string, rating: int])

        <hide>

        # compare the data to be sure...

        >>> rv = sorted(restored.create_vertices_frame().take(20).data)

        >>> gv = sorted(graph.create_vertices_frame().take(20).data)

        >>> assert rv == gv

        >>> re = sorted(restored.create_edges_frame().take(20).data)

        >>> ge = sorted(graph.create_edges_frame().take(20).data)

        >>> assert re == ge

        </hide>

    """
    def __init__(self, tc, source_or_vertices_frame, edges_frame=None):
        self._tc = tc
        self._scala = None

        # (note that the Scala code will validate appropriate frame schemas)

        if isinstance(source_or_vertices_frame, Frame):
            # Python Vertices and Edges Frames
            vertices_frame = source_or_vertices_frame
            require_type(edges_frame,
                         'edges_frame',
                         Frame,
                         "Providing a vertices frame requires also providing an edges frame")
            self._scala = self.create_scala_graph_from_scala_frames(self._tc,
                                                                    vertices_frame._scala,
                                                                    edges_frame._scala)
        else:
            source = source_or_vertices_frame
            require_type(edges_frame,
                         'edges_frame',
                         None,
                         'If edges_frames is provided, then a valid vertex frame must be provided as the first arg, instead of type %s' % type(source))
            if self._is_scala_graph(source):
                # Scala Graph
                self._scala = source
            elif isinstance(source, GraphFrame):
                # python GraphFrame
                scala_graphframe =  source._jvm_graph
                self._scala = self.create_scala_graph_from_scala_graphframe(self._tc, scala_graphframe)
            elif self._is_scala_graphframe(source):
                # scala GraphFrame
                self._scala = self.create_scala_graph_from_scala_graphframe(self._tc, source)
            else:
                raise TypeError("Cannot create from source type %s" % type(source))


    def __repr__(self):
        return self._scala.toString()

    @staticmethod
    def get_scala_graph_class(tc):
        """Gets reference to the sparktk scala Graph class"""
        return tc.sc._jvm.org.trustedanalytics.sparktk.graph.Graph

    @staticmethod
    def get_scala_graphframe_class(tc):
        """Gets reference to the scala GraphFrame class"""
        return tc.sc._jvm.org.graphframes.GraphFrame

    @staticmethod
    def create_scala_graph_from_scala_graphframe(tc, scala_graphframe):
        try:
            return tc.sc._jvm.org.trustedanalytics.sparktk.graph.Graph(scala_graphframe)
        except (Py4JJavaError, IllegalArgumentException) as e:
            raise ValueError(str(e))

    @staticmethod
    def create_scala_graph_from_scala_frames(tc, scala_vertices_frame, scala_edges_frame):
        try:
            return tc.sc._jvm.org.trustedanalytics.sparktk.graph.internal.constructors.FromFrames.create(scala_vertices_frame, scala_edges_frame)
        except (Py4JJavaError, IllegalArgumentException) as e:
            raise ValueError(str(e))

    # this one for the Loader:
    @staticmethod
    def _from_scala(tc, scala_graph):
        """creates a python Frame for the given scala Frame"""
        return Graph(tc, scala_graph)

    def _is_scala_graph(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self.get_scala_graph_class(self._tc))

    def _is_scala_graphframe(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self.get_scala_graphframe_class(self._tc))

    ##########################################################################
    # API
    ##########################################################################

    @property
    def graphframe(self):
        """The underlying graphframe object which exposes several methods and properties"""
        return _from_java_gf(self._scala.graphFrame(), self._tc.sql_context)

    def create_vertices_frame(self):
        """Creates a frame representing the vertices stored in this graph"""
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.graphFrame().vertices())

    def create_edges_frame(self):
        """Creates a frame representing the edges stored in this graph"""
        from sparktk.frame.frame import Frame
        return Frame(self._tc, self._scala.graphFrame().edges())

    # Graph Operations
    from sparktk.graph.ops.connected_components import connected_components
    from sparktk.graph.ops.clustering_coefficient import clustering_coefficient
    from sparktk.graph.ops.degrees import degrees
    from sparktk.graph.ops.export_to_orientdb import export_to_orientdb
    from sparktk.graph.ops.label_propagation import label_propagation
    from sparktk.graph.ops.page_rank import page_rank
    from sparktk.graph.ops.save import save
    from sparktk.graph.ops.triangle_count import triangle_count
    from sparktk.graph.ops.vertex_count import vertex_count
    from sparktk.graph.ops.weighted_degrees import weighted_degrees
