import logging
logger = logging.getLogger('sparktk')
from graphframes.graphframe import GraphFrame, _from_java_gf
from pyspark import SQLContext

#from sparktk.propobj import PropertiesObject

# import constructors for the API's sake (not actually dependencies of the Graph)
from sparktk.graph.constructors.create import create, create_from_spark_graphframe
# from sparktk.graph.constructors.import_orientdb import import_orientdb  (Wafaa, you get to uncomment this line)


#def get_example_graph_frame(tc):
    #return tc.sc._jvm.org.trustedanalytics.sparktk.graph.GraphHelp.getExampleGraphFrame()


class Graph(object):

    def __init__(self, tc, source):
        self._tc = tc
        self._scala = None
        self._python_graphframe = None
        if self._is_scala_graph(source):
            # Scala Graph
            self._scala = source
        elif self._is_scala_graphframe(source):
            # scala GraphFrame
            self._scala = self.create_scala_graph_from_scala_graphframe(self._tc, source)
        elif isinstance(source, GraphFrame):
            # python GraphFrame
            scala_graphframe =  source._jvm_graph
            self._scala = self.create_scala_graph_from_scala_graphframe(self._tc, scala_graphframe)
        else:
            raise ValueError("Cannot create from source %s" % source)

        sql_context = SQLContext(tc.sc)
        self._python_graphframe = _from_java_gf(self._scala.graphFrame(), sql_context)

    @staticmethod
    def create_scala_graph(sc, scala_graph_frame):
        """call constructor in JVM"""
        return sc._jvm.org.trustedanalytics.sparktk.graph.Graph(scala_graph_frame)

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
        #return tc.sc._jvm.org.trustedanalytics.sparktk.graph.GraphHelp.createGraph(scala_graphframe)
        return tc.sc._jvm.org.trustedanalytics.sparktk.graph.Graph(scala_graphframe)

    @staticmethod
    def create_scala_graph_from_scala_frames(tc, scala_vertex_frame, scala_edge_frame):
        return tc.sc._jvm.org.trustedanalytics.sparktk.graph.internal.constructors.FromFrames.create(scala_vertex_frame, scala_edge_frame)

    def _graph_to_scala(self, python_graph):
        """converts a Python Graph to a Scala Graph"""
        return self.create_scala_graph(self._tc.sc, python_graph._graph._jvm_graph)

    def _is_scala_graph(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self.get_scala_graph_class(self._tc))

    def _is_scala_graphframe(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self.get_scala_graphframe_class(self._tc))

    ##########################################################################
    # API
    ##########################################################################

    @property
    def graphframe(self):
        return self._python_graphframe

    from sparktk.graph.ops.vertex_count import vertex_count
