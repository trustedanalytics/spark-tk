package org.trustedanalytics.sparktk.graph.internal.constructors

import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.graph.Graph

object FromFrames {

  /**
   * Creates a sparktk Graph from two sparktk Frames
   *
   * @param vertexFrame - frame defining the vertices for the graph; schema must have a column named "id" which provides
   *                    unique vertex ID.  All other columns are treated as vertex properties.  If a column is also
   *                    found named "vertex", it will be used as a special label to denote the type of vertex,
   *                    for example, when interfacing with logic (such as a graph DB) which expects a specific vertex type
   * @param edgeFrame - frame defining the edges of the graph; schema must have columns names "src" and "dst" which
   *                  provide the vertex ids of the edge.  All other columns are treated as edge properties.  If a
   *                  column is also found named "edge", it will be used as a special label to denote the type of edge,
   *                  for example, when interfacing with logic (such as a graph DB) which expects a specific edge type
   * @return
   */
  def create(vertexFrame: Frame, edgeFrame: Frame): Graph = ???

}
